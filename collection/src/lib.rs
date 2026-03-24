mod client;
pub mod collectors;
pub mod sources;
mod sector_lookup;
mod sentiment;
mod types;
mod utils;

pub type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

pub use client::build_client;
pub use collectors::collect_ml_data;
pub use sources::{fetch_alpaca_stock_metrics, fetch_return_1d_from_snapshot};
pub use sector_lookup::{lookup_sector, lookup_sector_benchmark_symbol};
pub use sentiment::{average_article_sentiment, sentiment_analysis};
pub use types::{
    AlpacaStockMetrics, GlobeNewswireRelease, GoogleArticle, KalshiPrediction, MLData,
    NasdaqTradeHalt, PolymarketPrediction, PrNewswireRelease, SecFiling,
};
pub use utils::{normalize_ticker, parse_datetime_to_utc, parse_filing_datetime};
