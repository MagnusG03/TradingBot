mod market_data;
mod news;
mod prediction_markets;
mod reddit;
mod sec;

use reqwest::Client;

use crate::{AppResult, structures::FetchResult};

pub use market_data::{fetch_alpaca_stock_metrics, fetch_return_1d_from_snapshot};
pub use news::{fetch_globenewswire, fetch_google_news, fetch_nasdaq_trade_halt, fetch_prnewswire};
pub use prediction_markets::{fetch_kalshi, fetch_polymarket};
pub use reddit::{fetch_reddit, get_reddit_access_token};
pub use sec::{fetch_sec_edgar_all, fetch_sec_edgar_ticker, lookup_sec_cik};

pub async fn fetch(identifier: &str, client: &Client) -> AppResult<FetchResult> {
    if identifier.contains("reddit.com") {
        let client_id =
            std::env::var("REDDIT_CLIENT_ID").unwrap_or_else(|_| "your_client_id".to_string());
        let client_secret = std::env::var("REDDIT_CLIENT_SECRET")
            .unwrap_or_else(|_| "your_client_secret".to_string());
        let access_token = get_reddit_access_token(client, &client_id, &client_secret).await?;
        return Ok(FetchResult::Reddit(
            fetch_reddit(identifier, client, &access_token).await?,
        ));
    }

    if identifier.contains("polymarket.com") {
        return Ok(FetchResult::Polymarket(
            fetch_polymarket(identifier, client).await?,
        ));
    }

    if identifier.contains("kalshi.com") {
        return Ok(FetchResult::Kalshi(fetch_kalshi(identifier, client).await?));
    }

    if identifier.contains("sec.gov") {
        return Ok(FetchResult::SecEdgar(
            fetch_sec_edgar_all(identifier, client).await?,
        ));
    }

    if !identifier.is_empty() && identifier.len() <= 5 {
        return Ok(FetchResult::SecEdgar(
            fetch_sec_edgar_ticker(identifier, client).await?,
        ));
    }

    if identifier.contains("prnewswire.com") {
        return Ok(FetchResult::PrNewswire(
            fetch_prnewswire(client, identifier).await?,
        ));
    }

    if identifier.contains("nasdaqtrader.com") {
        return Ok(FetchResult::NasdaqTradeHalt(
            fetch_nasdaq_trade_halt(client, identifier).await?,
        ));
    }

    if identifier.contains("globenewswire.com") {
        return Ok(FetchResult::GlobeNewswire(
            fetch_globenewswire(client, identifier).await?,
        ));
    }

    if identifier.contains("google.com") {
        return Ok(FetchResult::GoogleNews(
            fetch_google_news(identifier, client).await?,
        ));
    }

    Err(std::io::Error::other("Unsupported identifier").into())
}
