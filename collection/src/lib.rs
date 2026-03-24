mod client;
mod collector;
pub mod fetchers;
mod sector_lookup;
mod sentiment;
mod structures;
mod utils;

pub type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

pub use client::build_client;
pub use collector::gather_data;
pub use fetchers::{fetch_alpaca_stock_metrics, fetch_return_1d_from_snapshot};
pub use sector_lookup::{
    build_market_news_url, build_sector_news_url, build_ticker_news_url, lookup_sector,
    lookup_sector_benchmark_symbol,
};
pub use sentiment::{average_article_sentiment, sentiment_analysis};
pub use structures::{
    AlpacaStockMetrics, FetchResult, GlobeNewswireRelease, GoogleArticle, KalshiPrediction, MLData,
    NasdaqTradeHalt, PolymarketPrediction, PrNewswireRelease, SecFiling,
};
pub use utils::{normalize_ticker, parse_datetime_to_utc, parse_filing_datetime};
