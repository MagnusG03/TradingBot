use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::{RequestBuilder, Response, StatusCode, header::HeaderMap};
use tokio::{
    sync::Mutex,
    time::{Duration, Instant, sleep, sleep_until},
};

use crate::AppResult;

const DEFAULT_RETRY_ATTEMPTS: usize = 4;
const RETRY_BUFFER_MS: u64 = 250;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RequestSource {
    AlpacaMarketData,
    GoogleNewsRss,
    NasdaqTradeHaltsRss,
    PublicRss,
    SecEdgar,
}

struct ThrottleState {
    next_allowed_by_source: Mutex<HashMap<RequestSource, Instant>>,
}

pub(crate) async fn send_with_throttle(
    request: RequestBuilder,
    source: RequestSource,
) -> AppResult<Response> {
    for attempt in 0..DEFAULT_RETRY_ATTEMPTS {
        wait_for_turn(source).await;

        let Some(current_request) = request.try_clone() else {
            return Err("Unable to clone request for throttled retry handling.".into());
        };

        match current_request.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }

                if should_retry_status(response.status()) && attempt + 1 < DEFAULT_RETRY_ATTEMPTS {
                    sleep(retry_delay(&response, source, attempt)).await;
                    continue;
                }

                return Err(response.error_for_status().unwrap_err().into());
            }
            Err(error) => {
                if (error.is_connect() || error.is_timeout())
                    && attempt + 1 < DEFAULT_RETRY_ATTEMPTS
                {
                    sleep(fallback_retry_delay(source, attempt)).await;
                    continue;
                }

                return Err(error.into());
            }
        }
    }

    Err("Request exhausted all retry attempts.".into())
}

async fn wait_for_turn(source: RequestSource) {
    let now = Instant::now();
    let min_interval = min_interval_for(source);
    let state = throttle_state();
    let mut next_allowed_by_source = state.next_allowed_by_source.lock().await;
    let scheduled = next_allowed_by_source
        .get(&source)
        .copied()
        .filter(|instant| *instant > now)
        .unwrap_or(now);

    next_allowed_by_source.insert(source, scheduled + min_interval);
    drop(next_allowed_by_source);

    if scheduled > now {
        sleep_until(scheduled).await;
    }
}

fn throttle_state() -> &'static ThrottleState {
    static STATE: OnceLock<ThrottleState> = OnceLock::new();

    STATE.get_or_init(|| ThrottleState {
        next_allowed_by_source: Mutex::new(HashMap::new()),
    })
}

fn should_retry_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retry_delay(response: &Response, source: RequestSource, attempt: usize) -> Duration {
    retry_after_delay(response.headers())
        .or_else(|| ratelimit_reset_delay(response.headers()))
        .unwrap_or_else(|| fallback_retry_delay(source, attempt))
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| Duration::from_secs(seconds) + Duration::from_millis(RETRY_BUFFER_MS))
}

fn ratelimit_reset_delay(headers: &HeaderMap) -> Option<Duration> {
    let reset_epoch_seconds = headers
        .get("x-ratelimit-reset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    let now_epoch_seconds = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let wait_seconds = reset_epoch_seconds.saturating_sub(now_epoch_seconds);

    Some(Duration::from_secs(wait_seconds) + Duration::from_millis(RETRY_BUFFER_MS))
}

fn fallback_retry_delay(source: RequestSource, attempt: usize) -> Duration {
    let multiplier = 1_u32 << attempt.min(5);
    min_interval_for(source).saturating_mul(multiplier)
}

fn min_interval_for(source: RequestSource) -> Duration {
    match source {
        RequestSource::AlpacaMarketData => env_duration_ms("TB_ALPACA_MIN_INTERVAL_MS", 400),
        RequestSource::GoogleNewsRss => env_duration_ms("TB_GOOGLE_NEWS_MIN_INTERVAL_MS", 2000),
        RequestSource::NasdaqTradeHaltsRss => {
            env_duration_ms("TB_NASDAQ_RSS_MIN_INTERVAL_MS", 2_000)
        }
        RequestSource::PublicRss => env_duration_ms("TB_PUBLIC_RSS_MIN_INTERVAL_MS", 2000),
        RequestSource::SecEdgar => env_duration_ms("TB_SEC_MIN_INTERVAL_MS", 200),
    }
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    let millis = std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_ms);

    Duration::from_millis(millis)
}
