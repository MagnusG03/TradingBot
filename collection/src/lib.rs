mod client;
pub mod collectors;
mod sector_lookup;
mod sentiment;
pub mod sources;
mod types;
mod utils;

pub type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

pub use client::build_client;
pub use collectors::{
    collect_ml_data, collect_ml_data_now, collect_ml_training_data,
    collect_ml_training_data_for_ticker,
};
pub use sector_lookup::{lookup_sector, lookup_sector_benchmark_symbol};
pub use sentiment::{article_sentiment, average_article_sentiment, sentiment_analysis};
pub use sources::{
    DailyBar, PriceFrame, StockSnapshot, fetch_alpaca_stock_metrics, fetch_daily_bars,
    fetch_price_frame, fetch_return_1d_from_snapshot, fetch_stock_snapshot,
};
pub use types::{
    AggregatorInput, AlpacaStockMetrics, EarningsSpecialistInput, GeneralistInput,
    GlobeNewswireRelease, GoogleArticle, KalshiPrediction, MLData, MLTrainingRecord,
    MLTrainingTargets, MarketRegime, MarketSession, NasdaqTradeHalt, NewsCategory,
    NewsEventSpecialistInput, PolymarketPrediction, PrNewswireRelease, RegimeSpecialistInput,
    SecFiling, SharedContext, TechnicalSpecialistInput,
};
pub use utils::{normalize_ticker, parse_datetime_to_utc, parse_filing_datetime};
