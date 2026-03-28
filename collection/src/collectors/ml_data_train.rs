use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::Client;
use tokio::task::JoinSet;

use crate::{
    AppResult, build_client, lookup_sector_benchmark_symbol,
    sources::{
        DailyBar, PriceFrame, fetch_daily_bars_with_lookback, fetch_google_news_range,
        fetch_nasdaq_trade_halts_for_date, fetch_sec_edgar_ticker_since,
    },
    types::{GoogleArticle, MLTrainingRecord, MLTrainingTargets, NasdaqTradeHalt},
    utils::{normalize_ticker, parse_datetime_to_utc},
};

use super::ml_data_now::{
    BENCHMARK_SYMBOL, GOOGLE_NEWS_QUERY_ITEM_CAP, QQQ_SYMBOL, REQUIRED_HISTORY_BARS,
    SampleSourceData, SourceAvailability, build_ml_data_from_source_data,
    canonical_close_sample_timestamp, close_sample_price_frame,
};

const FORECAST_HORIZON_BARS: usize = 7;
const LARGE_MOVE_THRESHOLD_7D: f64 = 0.05;
const TRADEABLE_EDGE_THRESHOLD_7D: f64 = 0.02;
const MIN_SIGNAL_FRIENDLY_TREND_7D: f64 = 0.01;
const DEFAULT_TRAINING_SAMPLE_LOOKBACK_DAYS: i64 = 3650;
const FILING_LOOKBACK_BUFFER_DAYS: i64 = 120;
const NEWS_LOOKBACK_DAYS: i64 = 3;
const NEWS_CHUNK_DAYS: i64 = 7;
const HALT_FETCH_BATCH_SIZE: usize = 16;
const NEWS_FETCH_BATCH_SIZE: usize = 8;
const EXTRA_PRICE_HISTORY_BUFFER_DAYS: i64 = 180;

#[derive(Clone)]
struct AlignedBars {
    timestamp: DateTime<Utc>,
    date: NaiveDate,
    ticker: DailyBar,
    benchmark: DailyBar,
    qqq: DailyBar,
    sector: Option<DailyBar>,
}

#[derive(Clone, Default)]
struct TradeHaltDay {
    halts: Vec<NasdaqTradeHalt>,
    available: bool,
}

#[derive(Clone)]
struct HistoricalNewsChunk {
    start: NaiveDate,
    end_exclusive: NaiveDate,
    articles: Vec<GoogleArticle>,
    available: bool,
    truncated: bool,
}

#[derive(Default)]
struct SampleNewsWindow {
    articles: Vec<GoogleArticle>,
    available: bool,
    truncated: bool,
}

pub async fn collect_ml_training_data<T>(tickers: &[T]) -> Vec<MLTrainingRecord>
where
    T: AsRef<str>,
{
    let mut records = Vec::new();
    let mut seen = HashSet::new();

    for ticker in tickers {
        let normalized_ticker = normalize_ticker(ticker.as_ref());
        if normalized_ticker.is_empty() || !seen.insert(normalized_ticker.clone()) {
            continue;
        }

        records.extend(collect_ml_training_data_for_ticker(&normalized_ticker).await);
    }

    records.sort_by(|left, right| {
        left.as_of_timestamp_utc
            .cmp(&right.as_of_timestamp_utc)
            .then_with(|| left.ticker.cmp(&right.ticker))
    });
    records
}

pub async fn collect_ml_training_data_for_ticker(ticker: &str) -> Vec<MLTrainingRecord> {
    let client = build_client();
    let normalized_ticker = normalize_ticker(ticker);
    if normalized_ticker.is_empty() {
        return Vec::new();
    }

    let training_sample_lookback_days = training_sample_lookback_days();
    let price_history_lookback_days =
        training_sample_lookback_days + EXTRA_PRICE_HISTORY_BUFFER_DAYS;
    let now = Utc::now();
    let training_start_date = now
        .date_naive()
        .checked_sub_signed(Duration::days(training_sample_lookback_days))
        .unwrap_or(NaiveDate::MIN);
    let filing_min_date = training_start_date
        .checked_sub_signed(Duration::days(FILING_LOOKBACK_BUFFER_DAYS))
        .unwrap_or(training_start_date);
    let sector_benchmark_symbol = lookup_sector_benchmark_symbol(&normalized_ticker, None);
    let (
        ticker_bars_result,
        benchmark_bars_result,
        qqq_bars_result,
        sector_bars_result,
        filings_result,
    ) = tokio::join!(
        fetch_daily_bars_with_lookback(&normalized_ticker, &client, price_history_lookback_days),
        fetch_daily_bars_with_lookback(BENCHMARK_SYMBOL, &client, price_history_lookback_days),
        fetch_daily_bars_with_lookback(QQQ_SYMBOL, &client, price_history_lookback_days),
        fetch_optional_daily_bars(
            sector_benchmark_symbol,
            &client,
            price_history_lookback_days,
        ),
        fetch_sec_edgar_ticker_since(&normalized_ticker, &client, Some(filing_min_date)),
    );

    let Ok(ticker_bars) = ticker_bars_result else {
        return Vec::new();
    };
    let Ok(benchmark_bars) = benchmark_bars_result else {
        return Vec::new();
    };
    let Ok(qqq_bars) = qqq_bars_result else {
        return Vec::new();
    };

    let has_sector_benchmark = sector_benchmark_symbol.is_some();
    let sector_available = matches!(sector_bars_result, Ok(Some(_)));
    let sector_bars = match sector_bars_result {
        Ok(Some(bars)) => bars,
        _ => Vec::new(),
    };
    let filings_available = filings_result.is_ok();
    let filings = filings_result.unwrap_or_default();
    let aligned = align_historical_bars(
        &ticker_bars,
        &benchmark_bars,
        &qqq_bars,
        sector_available.then_some(sector_bars.as_slice()),
    );

    if aligned.len() <= REQUIRED_HISTORY_BARS + FORECAST_HORIZON_BARS {
        return Vec::new();
    }

    let sample_indices: Vec<usize> = (REQUIRED_HISTORY_BARS..aligned.len() - FORECAST_HORIZON_BARS)
        .filter(|index| aligned[*index].date >= training_start_date)
        .collect();

    if sample_indices.is_empty() {
        return Vec::new();
    }

    let sample_dates: Vec<NaiveDate> = sample_indices
        .iter()
        .map(|index| aligned[*index].date)
        .collect();
    let trade_halt_cache = fetch_trade_halts_for_dates(&sample_dates, &client).await;
    let earliest_news_date = sample_dates
        .first()
        .and_then(|date| date.checked_sub_signed(Duration::days(NEWS_LOOKBACK_DAYS)))
        .unwrap_or(training_start_date);
    let latest_news_end = sample_dates
        .last()
        .and_then(|date| date.checked_add_signed(Duration::days(1)))
        .unwrap_or(training_start_date);
    let news_chunks = fetch_historical_news_chunks(
        &normalized_ticker,
        earliest_news_date,
        latest_news_end,
        &client,
    )
    .await;

    let mut records = Vec::new();

    for current_index in sample_indices {
        let current = &aligned[current_index];
        let history = &aligned[..current_index];
        let ticker_history: Vec<_> = history.iter().map(|bars| bars.ticker.clone()).collect();
        let benchmark_history: Vec<_> = history.iter().map(|bars| bars.benchmark.clone()).collect();
        let qqq_history: Vec<_> = history.iter().map(|bars| bars.qqq.clone()).collect();
        let sector_history: Vec<_> = history
            .iter()
            .filter_map(|bars| bars.sector.clone())
            .collect();

        let ticker_frame =
            close_sample_price_frame(&normalized_ticker, &ticker_history, &current.ticker);
        let benchmark_frame =
            close_sample_price_frame(BENCHMARK_SYMBOL, &benchmark_history, &current.benchmark);
        let qqq_frame = close_sample_price_frame(QQQ_SYMBOL, &qqq_history, &current.qqq);
        let sector_frame = match current.sector.as_ref() {
            Some(sector_bar) => close_sample_price_frame(
                sector_benchmark_symbol.unwrap_or_default(),
                &sector_history,
                sector_bar,
            ),
            None => PriceFrame::default(),
        };
        let halt_day = trade_halt_cache
            .get(&current.date)
            .cloned()
            .unwrap_or_default();
        let news_window = sample_news_window(&news_chunks, current.timestamp);
        let source_availability = SourceAvailability {
            ticker_frame: true,
            benchmark_frame: true,
            qqq_frame: true,
            sector_frame: !has_sector_benchmark || sector_available,
            filings: filings_available,
            trade_halts: halt_day.available,
            news_1h: news_window.available,
            news_6h: news_window.available,
            news_24h: news_window.available,
            news_3d: news_window.available,
        };
        let data = build_ml_data_from_source_data(&SampleSourceData {
            now: current.timestamp,
            ticker: &normalized_ticker,
            ticker_frame: &ticker_frame,
            benchmark_frame: &benchmark_frame,
            qqq_frame: &qqq_frame,
            sector_frame: &sector_frame,
            sector_available,
            has_sector_benchmark,
            filings: &filings,
            trade_halts: &halt_day.halts,
            articles: &news_window.articles,
            news_source_truncated: news_window.truncated,
            source_availability,
        });
        let targets = build_training_targets(&aligned, current_index);

        records.push(MLTrainingRecord::from_ml_data(
            normalized_ticker.clone(),
            current.date.to_string(),
            current.timestamp.timestamp(),
            data,
            targets,
        ));
    }

    records
}

async fn fetch_optional_daily_bars(
    symbol: Option<&'static str>,
    client: &Client,
    lookback_days: i64,
) -> AppResult<Option<Vec<DailyBar>>> {
    match symbol {
        Some(symbol) => fetch_daily_bars_with_lookback(symbol, client, lookback_days)
            .await
            .map(Some),
        None => Ok(None),
    }
}

async fn fetch_trade_halts_for_dates(
    dates: &[NaiveDate],
    client: &Client,
) -> HashMap<NaiveDate, TradeHaltDay> {
    let mut unique_dates = dates.to_vec();
    unique_dates.sort();
    unique_dates.dedup();

    let mut by_date = HashMap::new();

    for chunk in unique_dates.chunks(HALT_FETCH_BATCH_SIZE) {
        let mut set = JoinSet::new();

        for date in chunk {
            let client = client.clone();
            let date = *date;
            set.spawn(async move {
                (
                    date,
                    fetch_nasdaq_trade_halts_for_date(&client, date)
                        .await
                        .map(|halts| TradeHaltDay {
                            halts,
                            available: true,
                        })
                        .unwrap_or_default(),
                )
            });
        }

        while let Some(result) = set.join_next().await {
            if let Ok((date, halts)) = result {
                by_date.insert(date, halts);
            }
        }
    }

    by_date
}

async fn fetch_historical_news_chunks(
    ticker: &str,
    start: NaiveDate,
    end_exclusive: NaiveDate,
    client: &Client,
) -> Vec<HistoricalNewsChunk> {
    let mut ranges = Vec::new();
    let mut chunk_start = start;

    while chunk_start < end_exclusive {
        let chunk_end = chunk_start
            .checked_add_signed(Duration::days(NEWS_CHUNK_DAYS))
            .map(|date| date.min(end_exclusive))
            .unwrap_or(end_exclusive);
        ranges.push((chunk_start, chunk_end));
        chunk_start = chunk_end;
    }

    let mut chunks = Vec::new();

    for range_batch in ranges.chunks(NEWS_FETCH_BATCH_SIZE) {
        let mut set = JoinSet::new();

        for (chunk_start, chunk_end) in range_batch {
            let client = client.clone();
            let ticker = ticker.to_string();
            let chunk_start = *chunk_start;
            let chunk_end = *chunk_end;
            set.spawn(async move {
                let result =
                    fetch_google_news_range(&ticker, chunk_start, chunk_end, &client).await;

                match result {
                    Ok(articles) => HistoricalNewsChunk {
                        start: chunk_start,
                        end_exclusive: chunk_end,
                        truncated: articles.len() >= GOOGLE_NEWS_QUERY_ITEM_CAP,
                        articles,
                        available: true,
                    },
                    Err(_) => HistoricalNewsChunk {
                        start: chunk_start,
                        end_exclusive: chunk_end,
                        articles: Vec::new(),
                        available: false,
                        truncated: false,
                    },
                }
            });
        }

        while let Some(result) = set.join_next().await {
            if let Ok(chunk) = result {
                chunks.push(chunk);
            }
        }
    }

    chunks.sort_by_key(|chunk| chunk.start);
    chunks
}

fn sample_news_window(
    chunks: &[HistoricalNewsChunk],
    sample_time: DateTime<Utc>,
) -> SampleNewsWindow {
    let window_start_date = sample_time
        .checked_sub_signed(Duration::days(NEWS_LOOKBACK_DAYS))
        .map(|time| time.date_naive())
        .unwrap_or(sample_time.date_naive());
    let window_end_exclusive = sample_time
        .date_naive()
        .checked_add_signed(Duration::days(1))
        .unwrap_or(sample_time.date_naive());
    let relevant_chunks: Vec<&HistoricalNewsChunk> = chunks
        .iter()
        .filter(|chunk| {
            chunk.start < window_end_exclusive && chunk.end_exclusive > window_start_date
        })
        .collect();

    if relevant_chunks.is_empty() {
        return SampleNewsWindow::default();
    }

    SampleNewsWindow {
        articles: relevant_chunks
            .iter()
            .flat_map(|chunk| chunk.articles.iter().cloned())
            .collect(),
        available: relevant_chunks.iter().all(|chunk| chunk.available),
        truncated: relevant_chunks.iter().any(|chunk| chunk.truncated),
    }
}

fn align_historical_bars(
    ticker_bars: &[DailyBar],
    benchmark_bars: &[DailyBar],
    qqq_bars: &[DailyBar],
    sector_bars: Option<&[DailyBar]>,
) -> Vec<AlignedBars> {
    let benchmark_by_date = bars_by_date(benchmark_bars);
    let qqq_by_date = bars_by_date(qqq_bars);
    let sector_by_date = sector_bars.map(bars_by_date);
    let mut aligned = Vec::new();

    for ticker_bar in ticker_bars {
        let Some(parsed_timestamp) = parse_datetime_to_utc(&ticker_bar.timestamp) else {
            continue;
        };
        let date = parsed_timestamp.date_naive();
        let timestamp = canonical_close_sample_timestamp(date);
        let Some(benchmark_bar) = benchmark_by_date.get(&date).cloned() else {
            continue;
        };
        let Some(qqq_bar) = qqq_by_date.get(&date).cloned() else {
            continue;
        };
        let sector_bar = match sector_by_date.as_ref() {
            Some(by_date) => by_date.get(&date).cloned(),
            None => None,
        };

        if sector_by_date.is_some() && sector_bar.is_none() {
            continue;
        }

        aligned.push(AlignedBars {
            timestamp,
            date,
            ticker: ticker_bar.clone(),
            benchmark: benchmark_bar,
            qqq: qqq_bar,
            sector: sector_bar,
        });
    }

    aligned.sort_by_key(|bars| bars.timestamp);
    aligned
}

fn bars_by_date(bars: &[DailyBar]) -> HashMap<NaiveDate, DailyBar> {
    let mut by_date = HashMap::new();

    for bar in bars {
        let Some(timestamp) = parse_datetime_to_utc(&bar.timestamp) else {
            continue;
        };
        by_date.insert(timestamp.date_naive(), bar.clone());
    }

    by_date
}

fn build_training_targets(aligned: &[AlignedBars], current_index: usize) -> MLTrainingTargets {
    let current = &aligned[current_index];
    let future = &aligned[current_index + FORECAST_HORIZON_BARS];
    let future_window = &aligned[current_index + 1..=current_index + FORECAST_HORIZON_BARS];
    let future_return_7d = safe_ratio(future.ticker.close, current.ticker.close);
    let future_benchmark_return_7d = safe_ratio(future.benchmark.close, current.benchmark.close);
    let future_qqq_return_7d = safe_ratio(future.qqq.close, current.qqq.close);
    let expected_excess_return_7d = future_return_7d - future_benchmark_return_7d;
    let predicted_volatility_7d =
        forward_realized_volatility(current.ticker.close, future_window, |bars| {
            bars.ticker.close
        });
    let benchmark_volatility_7d =
        forward_realized_volatility(current.benchmark.close, future_window, |bars| {
            bars.benchmark.close
        });

    MLTrainingTargets {
        future_return_7d: future_return_7d as f32,
        future_benchmark_return_7d: future_benchmark_return_7d as f32,
        future_qqq_return_7d: future_qqq_return_7d as f32,
        expected_excess_return_7d: expected_excess_return_7d as f32,
        prob_outperform_7d: expected_excess_return_7d > 0.0,
        prob_large_move_7d: future_return_7d.abs() >= LARGE_MOVE_THRESHOLD_7D,
        prob_signal_friendly: future_benchmark_return_7d.abs()
            >= benchmark_volatility_7d.max(MIN_SIGNAL_FRIENDLY_TREND_7D),
        prob_risk_on: future_benchmark_return_7d > 0.0 && future_qqq_return_7d > 0.0,
        prob_tradeable_long_7d: expected_excess_return_7d >= TRADEABLE_EDGE_THRESHOLD_7D
            && future_return_7d > 0.0,
        prob_tradeable_short_7d: expected_excess_return_7d <= -TRADEABLE_EDGE_THRESHOLD_7D
            && future_return_7d < 0.0,
        predicted_volatility_7d: predicted_volatility_7d as f32,
    }
}

fn forward_realized_volatility(
    current_close: f64,
    future_window: &[AlignedBars],
    selector: impl Fn(&AlignedBars) -> f64,
) -> f64 {
    let mut closes = Vec::with_capacity(future_window.len() + 1);
    closes.push(current_close);
    closes.extend(future_window.iter().map(selector));

    let returns = log_returns(&closes);
    std_dev(&returns)
}

fn log_returns(series: &[f64]) -> Vec<f64> {
    series
        .windows(2)
        .filter_map(|window| {
            let previous = window[0];
            let current = window[1];
            (previous > 0.0 && current > 0.0).then(|| (current / previous).ln())
        })
        .collect()
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() as f64 - 1.0);

    variance.sqrt()
}

fn safe_ratio(current: f64, base: f64) -> f64 {
    if current > 0.0 && base > 0.0 {
        (current / base) - 1.0
    } else {
        0.0
    }
}

fn training_sample_lookback_days() -> i64 {
    std::env::var("TB_TRAINING_LOOKBACK_DAYS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .map(|days| days.max(365))
        .unwrap_or(DEFAULT_TRAINING_SAMPLE_LOOKBACK_DAYS)
}
