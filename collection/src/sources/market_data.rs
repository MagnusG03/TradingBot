use chrono::{Duration, Utc};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;

use crate::{AppResult, types::AlpacaStockMetrics, utils::normalize_ticker};

const HISTORY_LOOKBACK_DAYS: i64 = 240;
const HISTORY_LIMIT: &str = "180";

#[derive(Debug, Clone)]
pub struct DailyBar {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Default)]
pub struct StockSnapshot {
    pub latest_price: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub prev_close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PriceFrame {
    pub symbol: String,
    pub snapshot: StockSnapshot,
    pub bars: Vec<DailyBar>,
}

#[derive(Debug, Deserialize)]
struct AlpacaTrade {
    p: f64,
}

#[derive(Debug, Deserialize)]
struct BarsResponse {
    bars: Vec<AlpacaBar>,
}

#[derive(Debug, Deserialize)]
struct AlpacaBar {
    t: String,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    v: f64,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(rename = "latestTrade")]
    latest_trade: Option<AlpacaTrade>,
    #[serde(rename = "dailyBar")]
    daily_bar: Option<SnapshotBar>,
    #[serde(rename = "prevDailyBar")]
    prev_daily_bar: Option<SnapshotBar>,
}

#[derive(Debug, Deserialize)]
struct SnapshotBar {
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    v: f64,
}

pub async fn fetch_price_frame(symbol: &str, client: &Client) -> AppResult<PriceFrame> {
    let symbol = normalize_ticker(symbol);
    let (snapshot, bars) = tokio::try_join!(
        fetch_stock_snapshot(&symbol, client),
        fetch_daily_bars(&symbol, client)
    )?;

    Ok(PriceFrame {
        symbol,
        snapshot,
        bars,
    })
}

pub async fn fetch_stock_snapshot(symbol: &str, client: &Client) -> AppResult<StockSnapshot> {
    let symbol = normalize_ticker(symbol);
    let (api_key, api_secret) = alpaca_credentials()?;

    let snapshot: Snapshot = with_alpaca_auth(
        client.get(format!(
            "https://data.alpaca.markets/v2/stocks/{symbol}/snapshot"
        )),
        &api_key,
        &api_secret,
    )
    .query(&[("feed", "iex")])
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;

    let latest_price = snapshot
        .latest_trade
        .map(|trade| trade.p)
        .or_else(|| snapshot.daily_bar.as_ref().map(|bar| bar.c))
        .or_else(|| snapshot.prev_daily_bar.as_ref().map(|bar| bar.c))
        .ok_or_else(|| format!("No usable snapshot price for {symbol}"))?;
    let prev_close = snapshot
        .prev_daily_bar
        .as_ref()
        .map(|bar| bar.c)
        .unwrap_or(latest_price);
    let open = snapshot
        .daily_bar
        .as_ref()
        .map(|bar| bar.o)
        .unwrap_or(prev_close);
    let high = snapshot
        .daily_bar
        .as_ref()
        .map(|bar| bar.h)
        .unwrap_or(latest_price.max(prev_close));
    let low = snapshot
        .daily_bar
        .as_ref()
        .map(|bar| bar.l)
        .unwrap_or(latest_price.min(prev_close));
    let close = snapshot
        .daily_bar
        .as_ref()
        .map(|bar| bar.c)
        .unwrap_or(latest_price);
    let volume = snapshot.daily_bar.as_ref().map(|bar| bar.v).unwrap_or(0.0);

    Ok(StockSnapshot {
        latest_price,
        open,
        high,
        low,
        close,
        prev_close,
        volume,
    })
}

pub async fn fetch_daily_bars(symbol: &str, client: &Client) -> AppResult<Vec<DailyBar>> {
    let symbol = normalize_ticker(symbol);
    let (api_key, api_secret) = alpaca_credentials()?;
    let now = Utc::now();
    let start = (now - Duration::days(HISTORY_LOOKBACK_DAYS)).to_rfc3339();
    let end = (now - Duration::days(1)).to_rfc3339();

    let response: BarsResponse = with_alpaca_auth(
        client.get(format!("https://data.alpaca.markets/v2/stocks/{symbol}/bars")),
        &api_key,
        &api_secret,
    )
    .query(&[
        ("timeframe", "1Day"),
        ("start", start.as_str()),
        ("end", end.as_str()),
        ("adjustment", "raw"),
        ("feed", "iex"),
        ("limit", HISTORY_LIMIT),
    ])
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;

    Ok(response
        .bars
        .into_iter()
        .map(|bar| DailyBar {
            timestamp: bar.t,
            open: bar.o,
            high: bar.h,
            low: bar.l,
            close: bar.c,
            volume: bar.v,
        })
        .collect())
}

pub async fn fetch_return_1d_from_snapshot(symbol: &str, client: &Client) -> AppResult<f64> {
    let snapshot = fetch_stock_snapshot(symbol, client).await?;

    if snapshot.prev_close <= 0.0 {
        return Err(format!("Invalid previous close for {symbol}").into());
    }

    Ok((snapshot.latest_price / snapshot.prev_close) - 1.0)
}

pub async fn fetch_alpaca_stock_metrics(
    ticker: &str,
    client: &Client,
) -> AppResult<AlpacaStockMetrics> {
    let frame = fetch_price_frame(ticker, client).await?;

    if frame.snapshot.prev_close <= 0.0 {
        return Err(format!("Invalid previous close for {}", frame.symbol).into());
    }

    let mut closes: Vec<f64> = frame.bars.iter().map(|bar| bar.close).collect();
    closes.push(frame.snapshot.latest_price);

    let peak_price_30d = frame
        .bars
        .iter()
        .rev()
        .take(30)
        .map(|bar| bar.high)
        .chain(std::iter::once(frame.snapshot.high))
        .fold(f64::NEG_INFINITY, f64::max);

    Ok(AlpacaStockMetrics {
        current_price: frame.snapshot.latest_price,
        peak_price_30d: if peak_price_30d.is_finite() {
            peak_price_30d
        } else {
            frame.snapshot.latest_price
        },
        return_1d: (frame.snapshot.latest_price / frame.snapshot.prev_close) - 1.0,
        volatility_1d: calculate_volatility(&closes),
    })
}

fn alpaca_credentials() -> AppResult<(String, String)> {
    Ok((
        std::env::var("APCA_API_KEY_ID")?,
        std::env::var("APCA_API_SECRET_KEY")?,
    ))
}

fn with_alpaca_auth(request: RequestBuilder, api_key: &str, api_secret: &str) -> RequestBuilder {
    request
        .header("APCA-API-KEY-ID", api_key)
        .header("APCA-API-SECRET-KEY", api_secret)
}

fn calculate_volatility(closes: &[f64]) -> f64 {
    let log_returns: Vec<f64> = closes
        .windows(2)
        .filter_map(|window| {
            let previous = window[0];
            let current = window[1];
            (previous > 0.0 && current > 0.0).then(|| (current / previous).ln())
        })
        .collect();

    if log_returns.len() <= 1 {
        return 0.0;
    }

    let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance = log_returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (log_returns.len() as f64 - 1.0);

    variance.sqrt()
}
