use std::collections::HashMap;

use chrono::{Duration, Utc};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;

use crate::{AppResult, structures::AlpacaStockMetrics, utils::normalize_ticker};

#[derive(Debug, Deserialize)]
struct LatestTradesResponse {
    trades: HashMap<String, AlpacaTrade>,
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
    h: f64,
    c: f64,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(rename = "latestTrade")]
    latest_trade: Option<AlpacaTrade>,
    #[serde(rename = "prevDailyBar")]
    prev_daily_bar: Option<SnapshotBar>,
}

#[derive(Debug, Deserialize)]
struct SnapshotBar {
    c: f64,
}

pub async fn fetch_return_1d_from_snapshot(symbol: &str, client: &Client) -> AppResult<f64> {
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
        .ok_or_else(|| format!("No latest trade for {symbol}"))?
        .p;
    let prev_close = snapshot
        .prev_daily_bar
        .ok_or_else(|| format!("No previous daily bar for {symbol}"))?
        .c;

    if prev_close <= 0.0 {
        return Err(format!("Invalid previous close for {symbol}").into());
    }

    Ok((latest_price / prev_close) - 1.0)
}

pub async fn fetch_alpaca_stock_metrics(
    ticker: &str,
    client: &Client,
) -> AppResult<AlpacaStockMetrics> {
    let (api_key, api_secret) = alpaca_credentials()?;
    let symbol = normalize_ticker(ticker);
    let now = Utc::now();
    let start = (now - Duration::days(40)).to_rfc3339();
    let end = now.to_rfc3339();

    let latest_trade_fut = async {
        Ok::<LatestTradesResponse, Box<dyn std::error::Error>>(
            with_alpaca_auth(
                client.get("https://data.alpaca.markets/v2/stocks/trades/latest"),
                &api_key,
                &api_secret,
            )
            .query(&[("symbols", symbol.as_str()), ("feed", "iex")])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?,
        )
    };

    let bars_fut = async {
        Ok::<BarsResponse, Box<dyn std::error::Error>>(
            with_alpaca_auth(
                client.get(format!(
                    "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
                )),
                &api_key,
                &api_secret,
            )
            .query(&[
                ("timeframe", "1Day"),
                ("start", start.as_str()),
                ("end", end.as_str()),
                ("adjustment", "raw"),
                ("feed", "iex"),
                ("limit", "40"),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?,
        )
    };

    let (latest_trade_response, bars_response) = tokio::try_join!(latest_trade_fut, bars_fut)?;

    let current_price = latest_trade_response
        .trades
        .get(&symbol)
        .map(|trade| trade.p)
        .ok_or_else(|| format!("No latest trade returned for {symbol}"))?;

    if bars_response.bars.len() < 2 {
        return Err(format!("Not enough bars returned for {symbol}").into());
    }

    let closes: Vec<f64> = bars_response.bars.iter().map(|bar| bar.c).collect();
    let prev_close = closes[closes.len() - 2];

    Ok(AlpacaStockMetrics {
        current_price,
        peak_price_30d: bars_response
            .bars
            .iter()
            .map(|bar| bar.h)
            .fold(f64::NEG_INFINITY, f64::max),
        return_1d: (current_price / prev_close) - 1.0,
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
        .map(|window| (window[1] / window[0]).ln())
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
