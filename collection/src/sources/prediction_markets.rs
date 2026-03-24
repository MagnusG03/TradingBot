use reqwest::Client;
use serde::Deserialize;

use crate::{
    AppResult,
    types::{KalshiPrediction, PolymarketPrediction},
};

#[derive(Debug, Deserialize)]
struct PolymarketMarket {
    question: String,
    #[serde(default)]
    outcomes: Option<String>,
    #[serde(default, rename = "outcomePrices")]
    outcome_prices: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PolymarketEvent {
    #[serde(default)]
    markets: Vec<PolymarketMarket>,
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarket>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarket {
    title: String,
    #[serde(default)]
    subtitle: String,
    #[serde(default, rename = "yes_sub_title")]
    yes_sub_title: String,
    #[serde(default, rename = "no_sub_title")]
    no_sub_title: String,
    yes_bid: Option<i64>,
    no_bid: Option<i64>,
    #[serde(default, rename = "yes_bid_dollars")]
    yes_bid_dollars: Option<String>,
    #[serde(default, rename = "no_bid_dollars")]
    no_bid_dollars: Option<String>,
    #[serde(default, rename = "last_price_dollars")]
    last_price_dollars: Option<String>,
}

pub async fn fetch_polymarket(url: &str, client: &Client) -> AppResult<Vec<PolymarketPrediction>> {
    let (event_slug, market_slug) = split_polymarket_path(url);

    match market_slug {
        Some(market_slug) => {
            let market: PolymarketMarket = client
                .get(format!(
                    "https://gamma-api.polymarket.com/markets/slug/{market_slug}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            Ok(vec![to_polymarket_prediction(market)])
        }
        None => {
            let event: PolymarketEvent = client
                .get(format!(
                    "https://gamma-api.polymarket.com/events/slug/{event_slug}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            Ok(event
                .markets
                .into_iter()
                .map(to_polymarket_prediction)
                .collect())
        }
    }
}

pub async fn fetch_kalshi(url: &str, client: &Client) -> AppResult<Vec<KalshiPrediction>> {
    let event_ticker = url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();

    let response: MarketsResponse = client
        .get(format!(
            "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&event_ticker={event_ticker}"
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(response
        .markets
        .into_iter()
        .map(|market| {
            let subtitle = choose_subtitle(&market);

            KalshiPrediction {
                title: market.title,
                subtitle,
                yes_price: parse_price_dollars(market.yes_bid_dollars.as_deref())
                    .or_else(|| market.yes_bid.map(|value| value as f64 / 100.0))
                    .or_else(|| parse_price_dollars(market.last_price_dollars.as_deref()))
                    .unwrap_or(0.0),
                no_price: parse_price_dollars(market.no_bid_dollars.as_deref())
                    .or_else(|| market.no_bid.map(|value| value as f64 / 100.0))
                    .or_else(|| {
                        parse_price_dollars(market.last_price_dollars.as_deref())
                            .map(|last_price| 1.0 - last_price)
                    })
                    .unwrap_or(0.0),
            }
        })
        .collect())
}

fn split_polymarket_path(url: &str) -> (&str, Option<&str>) {
    let after_event = url
        .split("/event/")
        .nth(1)
        .unwrap_or("")
        .trim_end_matches('/');
    let mut parts = after_event.split('/');

    (parts.next().unwrap_or(""), parts.next())
}

fn to_polymarket_prediction(market: PolymarketMarket) -> PolymarketPrediction {
    PolymarketPrediction {
        question: market.question,
        outcomes: parse_json_string_list(market.outcomes.as_deref()),
        outcome_prices: parse_json_price_list(market.outcome_prices.as_deref()),
    }
}

fn parse_json_string_list(raw: Option<&str>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default()
}

fn parse_json_price_list(raw: Option<&str>) -> Vec<f64> {
    parse_json_string_list(raw)
        .into_iter()
        .filter_map(|value| value.parse::<f64>().ok())
        .collect()
}

fn parse_price_dollars(value: Option<&str>) -> Option<f64> {
    value.and_then(|raw| raw.parse::<f64>().ok())
}

fn choose_subtitle(market: &KalshiMarket) -> Option<String> {
    [
        market.subtitle.trim(),
        market.yes_sub_title.trim(),
        market.no_sub_title.trim(),
    ]
    .into_iter()
    .find(|value| !value.is_empty())
    .map(str::to_string)
}
