#[derive(Debug, Clone)]
pub struct MLData {
    pub generalist: GeneralistInput,
    pub technical: TechnicalSpecialistInput,
    pub earnings: EarningsSpecialistInput,
    pub news_event: NewsEventSpecialistInput,
    pub regime: RegimeSpecialistInput,
}

#[derive(Debug, Clone)]
pub struct PolymarketPrediction {
    pub question: String,
    pub outcomes: Vec<String>,
    pub outcome_prices: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct KalshiPrediction {
    pub title: String,
    pub subtitle: Option<String>,
    pub yes_price: f64,
    pub no_price: f64,
}

#[derive(Debug, Clone)]
pub struct SecFiling {
    pub ticker: String,
    pub company_name: String,
    pub cik: String,
    pub form: String,
    pub filing_date: String,
    pub acceptance_datetime: Option<String>,
    pub accession_number: String,
    pub primary_document: String,
    pub primary_doc_description: Option<String>,
    pub items: Option<String>,
    pub is_inline_xbrl: bool,
    pub filing_url: String,
}

#[derive(Debug, Clone)]
pub struct PrNewswireRelease {
    pub title: String,
    pub source_section: String,
    pub link: Option<String>,
    pub pub_date: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GlobeNewswireRelease {
    pub feed_name: String,
    pub title: String,
    pub link: String,
    pub pub_date: Option<String>,
    pub description: Option<String>,
    pub categories: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NasdaqTradeHalt {
    pub ticker: String,
    pub company_name: String,
    pub market: String,
    pub halt_date: String,
    pub halt_time: String,
    pub reason: String,
    pub resumption_date: Option<String>,
    pub resumption_quote_time: Option<String>,
    pub resumption_trade_time: Option<String>,
    pub pause_threshold_price: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GoogleArticle {
    pub title: String,
    pub link: String,
    pub pub_date: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AlpacaStockMetrics {
    pub current_price: f64,
    pub peak_price_30d: f64,
    pub return_1d: f64,
    pub volatility_1d: f64,
}

#[derive(Debug, Clone)]
pub struct GeneralistInput {
    pub ctx: SharedContext,

    pub return_1d: f32,
    pub return_3d: f32,
    pub return_5d: f32,
    pub return_10d: f32,
    pub return_20d: f32,

    pub sma_10_distance: f32,
    pub sma_20_distance: f32,
    pub sma_50_distance: f32,
    pub sma_20_slope: f32,
    pub sma_50_slope: f32,
    pub drawdown_from_20d_high: f32,
    pub drawdown_from_60d_high: f32,
    pub range_position_20d: f32,
    pub range_position_60d: f32,

    pub realized_vol_5d: f32,
    pub realized_vol_10d: f32,
    pub realized_vol_20d: f32,
    pub atr_14_pct: f32,

    pub volume_vs_5d_avg: f32,
    pub volume_vs_20d_avg: f32,
    pub dollar_volume_vs_20d_avg: f32,
    pub abnormal_volume_score: f32,

    pub benchmark_return_1d: f32,
    pub benchmark_return_5d: f32,
    pub benchmark_return_20d: f32,
    pub sector_return_1d: f32,
    pub sector_return_5d: f32,
    pub sector_return_20d: f32,
    pub excess_return_vs_benchmark_5d: f32,
    pub excess_return_vs_benchmark_20d: f32,
    pub excess_return_vs_sector_5d: f32,
    pub excess_return_vs_sector_20d: f32,
    pub rolling_beta_20d: f32,
    pub rolling_corr_benchmark_20d: f32,

    pub qqq_return_1d: f32,
    pub qqq_return_5d: f32,
    pub spy_return_1d: f32,
    pub spy_return_5d: f32,
    pub market_regime: MarketRegime,
    pub regime_confidence: f32,

    pub news_count_24h: u16,
    pub abnormal_news_count_24h: f32,
    pub avg_news_sentiment_24h: f32,
    pub sentiment_change_6h_vs_24h: f32,
    pub sentiment_dispersion_24h: f32,
    pub has_high_impact_news_24h: bool,
}

#[derive(Debug, Clone)]
pub struct TechnicalSpecialistInput {
    pub ctx: SharedContext,

    pub return_1d: f32,
    pub return_2d: f32,
    pub return_3d: f32,
    pub return_5d: f32,
    pub return_10d: f32,
    pub return_20d: f32,
    pub return_60d: f32,

    pub intraday_return_today: f32,
    pub overnight_return_today: f32,
    pub gap_from_prev_close: f32,
    pub hl_range_1d: f32,

    pub sma_5_distance: f32,
    pub sma_10_distance: f32,
    pub sma_20_distance: f32,
    pub sma_50_distance: f32,
    pub ema_10_distance: f32,
    pub ema_20_distance: f32,
    pub sma_20_slope: f32,
    pub sma_50_slope: f32,
    pub ema_20_slope: f32,
    pub trend_strength_20d: f32,

    pub price_zscore_10d: f32,
    pub price_zscore_20d: f32,
    pub return_zscore_5d: f32,
    pub return_zscore_20d: f32,
    pub mean_reversion_score_5d: f32,
    pub momentum_acceleration_5d_vs_20d: f32,

    pub distance_to_20d_high: f32,
    pub distance_to_60d_high: f32,
    pub distance_to_20d_low: f32,
    pub distance_to_60d_low: f32,
    pub drawdown_from_20d_high: f32,
    pub drawdown_from_60d_high: f32,
    pub range_position_20d: f32,
    pub range_position_60d: f32,

    pub realized_vol_5d: f32,
    pub realized_vol_10d: f32,
    pub realized_vol_20d: f32,
    pub downside_vol_10d: f32,
    pub upside_vol_10d: f32,
    pub atr_14_pct: f32,

    pub volume_vs_5d_avg: f32,
    pub volume_vs_20d_avg: f32,
    pub abnormal_volume_score: f32,
    pub volume_trend_5d: f32,
}

#[derive(Debug, Clone)]
pub struct EarningsSpecialistInput {
    pub ctx: SharedContext,

    pub days_since_last_earnings: Option<f32>,
    pub recent_earnings_filing_within_7d: bool,
    pub recent_earnings_filing_within_30d: bool,
    pub recent_earnings_filing_count_90d: u16,
    pub latest_earnings_filing_age_hours: Option<f32>,

    pub return_3d: f32,
    pub return_5d: f32,
    pub return_10d: f32,
    pub excess_return_vs_benchmark_5d: f32,
    pub realized_vol_5d: f32,
    pub realized_vol_10d: f32,
    pub drawdown_from_20d_high: f32,
    pub volume_vs_5d_avg: f32,
    pub abnormal_volume_score: f32,

    pub news_count_24h: u16,
    pub abnormal_news_count_24h: f32,
    pub avg_news_sentiment_24h: f32,
    pub sentiment_change_6h_vs_24h: f32,
    pub sentiment_dispersion_24h: f32,
    pub dominant_news_category: NewsCategory,
    pub has_high_impact_news_24h: bool,
}

#[derive(Debug, Clone)]
pub struct NewsEventSpecialistInput {
    pub ctx: SharedContext,

    pub news_count_1h: u16,
    pub news_count_6h: u16,
    pub news_count_24h: u16,
    pub news_count_3d: u16,
    pub abnormal_news_count_6h: f32,
    pub abnormal_news_count_24h: f32,

    pub avg_news_sentiment_1h: f32,
    pub avg_news_sentiment_6h: f32,
    pub avg_news_sentiment_24h: f32,
    pub sentiment_change_6h_vs_24h: f32,
    pub sentiment_dispersion_24h: f32,
    pub positive_news_ratio_24h: f32,
    pub negative_news_ratio_24h: f32,
    pub relevance_weighted_news_sentiment_24h: f32,

    pub hours_since_latest_news: Option<f32>,
    pub news_novelty_score_24h: Option<f32>,
    pub dominant_news_category: NewsCategory,
    pub has_high_impact_news_24h: bool,

    pub return_1d: f32,
    pub return_3d: f32,
    pub realized_vol_5d: f32,
    pub abnormal_volume_score: f32,
    pub excess_return_vs_benchmark_1d: f32,
}

#[derive(Debug, Clone)]
pub struct RegimeSpecialistInput {
    pub ctx: SharedContext,

    pub spy_return_1d: f32,
    pub spy_return_5d: f32,
    pub qqq_return_1d: f32,
    pub qqq_return_5d: f32,
    pub benchmark_return_20d: f32,

    pub rolling_beta_20d: f32,
    pub rolling_beta_60d: f32,
    pub rolling_corr_benchmark_20d: f32,
    pub idiosyncratic_vol_20d: f32,

    pub market_regime: MarketRegime,
    pub regime_confidence: f32,
}

#[derive(Debug, Clone)]
pub struct AggregatorInput {
    pub ctx: SharedContext,

    pub generalist_expected_excess_return_7d: f32,
    pub generalist_prob_outperform_7d: f32,
    pub generalist_confidence: f32,

    pub technical_expected_excess_return_7d: f32,
    pub technical_prob_outperform_7d: f32,
    pub technical_confidence: f32,

    pub earnings_expected_excess_return_7d: f32,
    pub earnings_prob_outperform_7d: f32,
    pub earnings_confidence: f32,

    pub news_expected_excess_return_7d: f32,
    pub news_prob_outperform_7d: f32,
    pub news_prob_large_move_7d: f32,
    pub news_confidence: f32,

    pub regime_prob_risk_on: f32,
    pub regime_prob_signal_friendly: f32,
    pub regime_confidence: f32,

    pub market_regime: MarketRegime,
    pub missing_feature_fraction: f32,
    pub data_quality_score: f32,
}

#[derive(Debug, Clone)]
pub struct SharedContext {
    pub timestamp_utc: i64,
    pub market_session: MarketSession,

    pub day_of_week: u8,
    pub day_of_month: u8,
    pub month: u8,
    pub week_of_year: u8,
    pub is_month_end_window: bool,
    pub is_quarter_end_window: bool,
    pub is_options_expiry_week: bool,

    pub current_price: f32,
    pub log_price: f32,

    pub missing_feature_fraction: f32,
    pub stale_data_flag: bool,
    pub data_quality_score: f32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MarketSession {
    PreMarket,
    Regular,
    AfterHours,
    Closed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MarketRegime {
    RiskOn,
    RiskOff,
    Neutral,
    HighVol,
    Trend,
    MeanReversion,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NewsCategory {
    None,
    Earnings,
    AnalystAction,
    Product,
    LegalRegulatory,
    Management,
    Macro,
    MAndA,
    Other,
}
