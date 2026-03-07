use reqwest::{Client, StatusCode};
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Debug)]
struct PredictionMarket {
    question: String,
    outcomes: Vec<String>,
    outcome_prices: Vec<f64>,
}

async fn fetch_polymarket(
    url: &str,
    client: &Client,
) -> Result<Vec<PredictionMarket>, Box<dyn Error>> {
    #[derive(Debug, Deserialize)]
    struct PolymarketMarket {
        id: String,
        slug: String,
        question: String,
        #[serde(default)]
        outcomes: Option<String>,
        #[serde(default, rename = "outcomePrices")]
        outcome_prices: Option<String>,
        #[serde(default, rename = "clobTokenIds")]
        clob_token_ids: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct PolymarketEvent {
        id: String,
        slug: String,
        title: String,
        #[serde(default)]
        markets: Vec<PolymarketMarket>,
    }

    let after_event = url
        .split("/event/")
        .nth(1)
        .unwrap_or("")
        .trim_end_matches('/');

    let parts: Vec<&str> = after_event.split('/').collect();

    let event_slug = parts.first().copied().unwrap_or("");
    let market_slug = parts.get(1).copied();

    if let Some(market_slug) = market_slug {
        let api_url = format!(
            "https://gamma-api.polymarket.com/markets/slug/{}",
            market_slug
        );

        let market = client
            .get(&api_url)
            .send()
            .await?
            .error_for_status()?
            .json::<PolymarketMarket>()
            .await?;

        let outcomes = market
            .outcomes
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_default();

        let outcome_prices = market
            .outcome_prices
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .map(|prices| {
                prices
                    .into_iter()
                    .filter_map(|p| p.parse::<f64>().ok())
                    .collect::<Vec<f64>>()
            })
            .unwrap_or_default();

        Ok(vec![PredictionMarket {
            question: market.question,
            outcomes,
            outcome_prices,
        }])
    } else {
        let api_url = format!(
            "https://gamma-api.polymarket.com/events/slug/{}",
            event_slug
        );

        let event = client
            .get(&api_url)
            .send()
            .await?
            .error_for_status()?
            .json::<PolymarketEvent>()
            .await?;

        let prediction_markets: Vec<PredictionMarket> = event
            .markets
            .into_iter()
            .map(|market| {
                let outcomes = market
                    .outcomes
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .unwrap_or_default();

                let outcome_prices = market
                    .outcome_prices
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .map(|prices| {
                        prices
                            .into_iter()
                            .filter_map(|p| p.parse::<f64>().ok())
                            .collect::<Vec<f64>>()
                    })
                    .unwrap_or_default();

                PredictionMarket {
                    question: market.question,
                    outcomes,
                    outcome_prices,
                }
            })
            .collect();

        Ok(prediction_markets)
    }
}

async fn fetch_kalshi(url: &str, client: &Client) -> Result<Vec<PredictionMarket>, Box<dyn Error>> {
    #[derive(Debug, Deserialize)]
    struct MarketsResponse {
        markets: Vec<KalshiMarket>,
    }

    #[derive(Debug, Deserialize)]
    struct KalshiMarket {
        title: String,
        yes_bid: Option<i64>,
        no_bid: Option<i64>,
    }

    #[derive(Debug, Deserialize)]
    struct PriceRange {
        start: String,
        end: String,
        step: String,
    }

    #[derive(Debug, Deserialize)]
    struct MveLeg {
        event_ticker: String,
        market_ticker: String,
        side: String,
        yes_settlement_value_dollars: String,
    }

    let event_ticker = url.rsplit('/').next().unwrap_or("").to_uppercase();

    let api_url = format!(
        "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&event_ticker={}",
        event_ticker
    );

    let response = client.get(&api_url).send().await?;

    let data: MarketsResponse = response.json().await?;

    let prediction_markets = data
        .markets
        .into_iter()
        .map(|market| PredictionMarket {
            question: market.title,
            outcomes: vec!["Yes".to_string(), "No".to_string()],
            outcome_prices: vec![
                market.yes_bid.unwrap_or(0) as f64 / 100.0,
                market.no_bid.unwrap_or(0) as f64 / 100.0,
            ],
        })
        .collect();

    Ok(prediction_markets)
}

async fn fetch(url: &str) -> Result<Vec<PredictionMarket>, Box<dyn Error>> {
    let client = Client::builder().user_agent("MagnusTradingBot").build()?;

    match () {
        _ if url.contains("polymarket.com") => fetch_polymarket(url, &client).await,
        _ if url.contains("kalshi.com") => fetch_kalshi(url, &client).await,
        _ => Err(std::io::Error::other("Unsupported URL").into()),
    }
}

#[tokio::main]
async fn main() {
    let url = "https://kalshi.com/markets/kxipo/ipos/kxipo-26";

    match fetch(url).await {
        Ok(markets) => {
            if markets.is_empty() {
                println!("No markets found.");
            } else {
                for market in markets {
                    println!("Question: {}", market.question);
                    println!("Outcomes: {:?}", market.outcomes);
                    println!("Outcome prices: {:?}", market.outcome_prices);
                }
            }
        }
        Err(e) => eprintln!("Error fetching market: {}", e),
    }
}
