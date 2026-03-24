use chrono::{DateTime, Utc};
use reqwest::Client;

use crate::{
    AppResult, average_article_sentiment, build_client, fetch_alpaca_stock_metrics,
    fetch_return_1d_from_snapshot,
    lookup_sector, lookup_sector_benchmark_symbol,
    sources::{fetch_google_news, fetch_nasdaq_trade_halt, fetch_sec_edgar_ticker},
    types::{GoogleArticle, MLData, SecFiling},
    utils::{normalize_ticker, parse_datetime_to_utc, parse_filing_datetime},
};

pub async fn collect_ml_data(ticker: &str) -> MLData {
    let client = build_client();
    let normalized_ticker = normalize_ticker(ticker);
    let sector = lookup_sector(&normalized_ticker);
    let sector_benchmark = lookup_sector_benchmark_symbol(&normalized_ticker, sector.as_deref());
    let market_benchmark_symbol = "SPY";
    let market_news_url = "https://news.google.com/rss/search?q=%22US+markets%22+OR+%22Wall+Street%22+OR+finance+OR+economy+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen";
    let trade_halts_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts";
    let ticker_news_url = format!(
        "https://news.google.com/rss/search?q={}+when%3A6h&hl=en-US&gl=US&ceid=US%3Aen",
        normalized_ticker
    );
    let sector_news_url = sector.as_deref().map(|sector| {
        let sector = sector.split_whitespace().collect::<Vec<_>>().join("+");

        format!(
            "https://news.google.com/rss/search?q=%22{0}%22+OR+%22{0}+sector%22+OR+%22{0}+stocks%22+OR+%22{0}+industry%22+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen",
            sector
        )
    });

    let (
        ticker_news,
        market_news,
        sector_news,
        sec_filings,
        trade_halts,
        price_metrics,
        sandp500_return,
        sector_return,
    ) = tokio::join!(
        fetch_google_news(&ticker_news_url, &client),
        fetch_google_news(market_news_url, &client),
        fetch_optional_google_news(sector_news_url.as_deref(), &client),
        fetch_sec_edgar_ticker(&normalized_ticker, &client),
        fetch_nasdaq_trade_halt(&client, trade_halts_url),
        fetch_alpaca_stock_metrics(&normalized_ticker, &client),
        fetch_return_1d_from_snapshot(market_benchmark_symbol, &client),
        fetch_sector_return(sector_benchmark, &client),
    );

    let ticker_articles = ticker_news.unwrap_or_default();
    let market_articles = market_news.unwrap_or_default();
    let sector_articles = sector_news.unwrap_or_default();
    let filings = sec_filings.unwrap_or_default();
    let halts = trade_halts.unwrap_or_default();
    let price_metrics = price_metrics.unwrap_or_default();
    let sandp500_return = sandp500_return.unwrap_or_default();
    let sector_return = sector_return.unwrap_or_default();
    let now = Utc::now();

    MLData {
        ticker: normalized_ticker.clone(),
        timestamp: now.to_rfc3339(),
        current_price: price_metrics.current_price,
        peak_price_30d: price_metrics.peak_price_30d,
        return_1d: price_metrics.return_1d,
        volatility_1d: price_metrics.volatility_1d,
        news_count_6h: ticker_articles.len() as u32,
        avg_news_sentiment_6h: average_article_sentiment(&ticker_articles),
        latest_news_age_minutes: latest_news_age_minutes(&ticker_articles, now),
        sec_filing_count_7d: recent_filing_count(&filings, now),
        has_8k_7d: has_recent_8k(&filings, now),
        latest_filing_age_hours: latest_filing_age_hours(&filings, now),
        has_recent_halt_1d: halts
            .iter()
            .any(|halt| halt.ticker.eq_ignore_ascii_case(&normalized_ticker)),
        sandp500_return_1d: sandp500_return,
        sector_return_1d: sector_return,
        general_market_sentiment_1d: average_article_sentiment(&market_articles),
        general_sector_sentiment_1d: average_article_sentiment(&sector_articles),
    }
}

async fn fetch_optional_google_news(
    url: Option<&str>,
    client: &Client,
) -> AppResult<Vec<GoogleArticle>> {
    match url {
        Some(url) => fetch_google_news(url, client).await,
        None => Ok(Vec::new()),
    }
}

async fn fetch_sector_return(symbol: Option<&'static str>, client: &Client) -> AppResult<f64> {
    match symbol {
        Some(symbol) => fetch_return_1d_from_snapshot(symbol, client).await,
        None => Ok(0.0),
    }
}

fn latest_news_age_minutes(articles: &[GoogleArticle], now: DateTime<Utc>) -> u32 {
    articles
        .iter()
        .filter_map(|article| article.pub_date.as_deref())
        .filter_map(parse_datetime_to_utc)
        .filter_map(|published_at| age_from(now, published_at).map(|age| age.num_minutes() as u32))
        .min()
        .unwrap_or(0)
}

fn recent_filing_count(filings: &[SecFiling], now: DateTime<Utc>) -> u32 {
    filings
        .iter()
        .filter_map(parse_filing_datetime)
        .filter(|filed_at| is_within_days(now, *filed_at, 7))
        .count() as u32
}

fn has_recent_8k(filings: &[SecFiling], now: DateTime<Utc>) -> bool {
    filings.iter().any(|filing| {
        matches!(filing.form.as_str(), "8-K" | "8-K/A")
            && parse_filing_datetime(filing)
                .map(|filed_at| is_within_days(now, filed_at, 7))
                .unwrap_or(false)
    })
}

fn latest_filing_age_hours(filings: &[SecFiling], now: DateTime<Utc>) -> u32 {
    filings
        .iter()
        .filter_map(parse_filing_datetime)
        .filter_map(|filed_at| age_from(now, filed_at).map(|age| age.num_hours() as u32))
        .min()
        .unwrap_or(0)
}

fn age_from(now: DateTime<Utc>, value: DateTime<Utc>) -> Option<chrono::Duration> {
    let age = now.signed_duration_since(value);
    (age.num_seconds() >= 0).then_some(age)
}

fn is_within_days(now: DateTime<Utc>, value: DateTime<Utc>, days: i64) -> bool {
    age_from(now, value)
        .map(|age| age.num_days() <= days)
        .unwrap_or(false)
}
