#[derive(Debug)]
pub struct MLData {
    pub ticker: String,
    pub timestamp: String,
    pub current_price: f64,
    pub peak_price_30d: f64,
    pub return_1d: f64,
    pub volatility_1d: f64,
    pub news_count_6h: u32,
    pub avg_news_sentiment_6h: f64,
    pub latest_news_age_minutes: u32,
    pub sec_filing_count_7d: u32,
    pub has_8k_7d: bool,
    pub latest_filing_age_hours: u32,
    pub has_recent_halt_1d: bool,
    pub sandp500_return_1d: f64,
    pub sector_return_1d: f64,
    pub general_market_sentiment_1d: f64,
    pub general_sector_sentiment_1d: f64,
}

#[derive(Debug)]
pub struct PolymarketPrediction {
    pub question: String,
    pub outcomes: Vec<String>,
    pub outcome_prices: Vec<f64>,
}

#[derive(Debug)]
pub struct KalshiPrediction {
    pub title: String,
    pub subtitle: Option<String>,
    pub yes_price: f64,
    pub no_price: f64,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct PrNewswireRelease {
    pub title: String,
    pub source_section: String,
    pub link: Option<String>,
    pub pub_date: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug)]
pub struct GlobeNewswireRelease {
    pub feed_name: String,
    pub title: String,
    pub link: String,
    pub pub_date: Option<String>,
    pub description: Option<String>,
    pub categories: Vec<String>,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct GoogleArticle {
    pub title: String,
    pub link: String,
    pub pub_date: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Default)]
pub struct AlpacaStockMetrics {
    pub current_price: f64,
    pub peak_price_30d: f64,
    pub return_1d: f64,
    pub volatility_1d: f64,
}
