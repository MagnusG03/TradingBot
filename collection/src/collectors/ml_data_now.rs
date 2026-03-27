use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use reqwest::Client;

use crate::{
    AppResult, article_sentiment, build_client, lookup_sector_benchmark_symbol,
    sources::{
        DailyBar, PriceFrame, fetch_google_news, fetch_nasdaq_trade_halts_for_date,
        fetch_price_frame, fetch_sec_edgar_ticker,
    },
    types::{
        AggregatorInput, EarningsSpecialistInput, GeneralistInput, GoogleArticle, MLData,
        MarketRegime, MarketSession, NasdaqTradeHalt, NewsCategory, NewsEventSpecialistInput,
        RegimeSpecialistInput, SecFiling, SharedContext, TechnicalSpecialistInput,
    },
    utils::{normalize_ticker, parse_datetime_to_utc, parse_filing_datetime},
};

pub(crate) const BENCHMARK_SYMBOL: &str = "SPY";
pub(crate) const QQQ_SYMBOL: &str = "QQQ";
pub(crate) const REQUIRED_HISTORY_BARS: usize = 60;
pub(crate) const GOOGLE_NEWS_QUERY_ITEM_CAP: usize = 100;
pub(crate) const CANONICAL_CLOSE_HOUR_ET: u32 = 16;
pub(crate) const CANONICAL_CLOSE_MINUTE_ET: u32 = 0;
const NEWS_TRUNCATION_QUALITY_PENALTY: f32 = 0.12;

#[derive(Clone, Default)]
struct PriceFeatures {
    current_price: f64,
    return_1d: f64,
    return_2d: f64,
    return_3d: f64,
    return_5d: f64,
    return_10d: f64,
    return_20d: f64,
    return_60d: f64,
    intraday_return_today: f64,
    overnight_return_today: f64,
    gap_from_prev_close: f64,
    hl_range_1d: f64,
    sma_5_distance: f64,
    sma_10_distance: f64,
    sma_20_distance: f64,
    sma_50_distance: f64,
    ema_10_distance: f64,
    ema_20_distance: f64,
    sma_20_slope: f64,
    sma_50_slope: f64,
    ema_20_slope: f64,
    trend_strength_20d: f64,
    price_zscore_10d: f64,
    price_zscore_20d: f64,
    return_zscore_5d: f64,
    return_zscore_20d: f64,
    mean_reversion_score_5d: f64,
    momentum_acceleration_5d_vs_20d: f64,
    distance_to_20d_high: f64,
    distance_to_60d_high: f64,
    distance_to_20d_low: f64,
    distance_to_60d_low: f64,
    drawdown_from_20d_high: f64,
    drawdown_from_60d_high: f64,
    range_position_20d: f64,
    range_position_60d: f64,
    realized_vol_5d: f64,
    realized_vol_10d: f64,
    realized_vol_20d: f64,
    downside_vol_10d: f64,
    upside_vol_10d: f64,
    atr_14_pct: f64,
    volume_vs_5d_avg: f64,
    volume_vs_20d_avg: f64,
    dollar_volume_vs_20d_avg: f64,
    abnormal_volume_score: f64,
    volume_trend_5d: f64,
}

#[derive(Clone, Default)]
struct RelativeFeatures {
    benchmark_return_1d: f64,
    benchmark_return_5d: f64,
    benchmark_return_20d: f64,
    sector_return_1d: f64,
    sector_return_5d: f64,
    sector_return_20d: f64,
    excess_return_vs_benchmark_1d: f64,
    excess_return_vs_benchmark_5d: f64,
    excess_return_vs_benchmark_20d: f64,
    excess_return_vs_sector_5d: f64,
    excess_return_vs_sector_20d: f64,
    rolling_beta_20d: f64,
    rolling_beta_60d: f64,
    rolling_corr_benchmark_20d: f64,
    idiosyncratic_vol_20d: f64,
    spy_return_1d: f64,
    spy_return_5d: f64,
    qqq_return_1d: f64,
    qqq_return_5d: f64,
}

#[derive(Clone, Default)]
struct FilingFeatures {
    days_since_last_earnings: Option<f32>,
    recent_earnings_filing_within_7d: bool,
    recent_earnings_filing_within_30d: bool,
    recent_earnings_filing_count_90d: u16,
    latest_earnings_filing_age_hours: Option<f32>,
}

#[derive(Clone)]
pub(crate) struct NewsFeatures {
    pub(crate) news_count_1h: u16,
    pub(crate) news_count_6h: u16,
    pub(crate) news_count_24h: u16,
    pub(crate) news_count_3d: u16,
    pub(crate) abnormal_news_count_6h: f64,
    pub(crate) abnormal_news_count_24h: f64,
    pub(crate) avg_news_sentiment_1h: f64,
    pub(crate) avg_news_sentiment_6h: f64,
    pub(crate) avg_news_sentiment_24h: f64,
    pub(crate) sentiment_change_6h_vs_24h: f64,
    pub(crate) sentiment_dispersion_24h: f64,
    pub(crate) positive_news_ratio_24h: f64,
    pub(crate) negative_news_ratio_24h: f64,
    pub(crate) relevance_weighted_news_sentiment_24h: f64,
    pub(crate) hours_since_latest_news: Option<f32>,
    pub(crate) news_novelty_score_24h: Option<f32>,
    pub(crate) dominant_news_category: NewsCategory,
    pub(crate) has_high_impact_news_24h: bool,
    pub(crate) source_truncated: bool,
}

impl Default for NewsFeatures {
    fn default() -> Self {
        Self {
            news_count_1h: 0,
            news_count_6h: 0,
            news_count_24h: 0,
            news_count_3d: 0,
            abnormal_news_count_6h: 0.0,
            abnormal_news_count_24h: 0.0,
            avg_news_sentiment_1h: 0.0,
            avg_news_sentiment_6h: 0.0,
            avg_news_sentiment_24h: 0.0,
            sentiment_change_6h_vs_24h: 0.0,
            sentiment_dispersion_24h: 0.0,
            positive_news_ratio_24h: 0.0,
            negative_news_ratio_24h: 0.0,
            relevance_weighted_news_sentiment_24h: 0.0,
            hours_since_latest_news: None,
            news_novelty_score_24h: None,
            dominant_news_category: NewsCategory::None,
            has_high_impact_news_24h: false,
            source_truncated: false,
        }
    }
}

#[derive(Clone, Default)]
struct QualityMetrics {
    missing_feature_fraction: f32,
    stale_data_flag: bool,
    data_quality_score: f32,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SourceAvailability {
    pub(crate) ticker_frame: bool,
    pub(crate) benchmark_frame: bool,
    pub(crate) qqq_frame: bool,
    pub(crate) sector_frame: bool,
    pub(crate) filings: bool,
    pub(crate) trade_halts: bool,
    pub(crate) news_1h: bool,
    pub(crate) news_6h: bool,
    pub(crate) news_24h: bool,
    pub(crate) news_3d: bool,
}

impl SourceAvailability {
    fn news_available(self) -> bool {
        self.news_1h || self.news_6h || self.news_24h || self.news_3d
    }

    pub(crate) fn success_count(self, has_sector_benchmark: bool) -> u32 {
        u32::from(self.ticker_frame)
            + u32::from(self.benchmark_frame)
            + u32::from(self.qqq_frame)
            + u32::from(has_sector_benchmark && self.sector_frame)
            + u32::from(self.filings)
            + u32::from(self.trade_halts)
            + u32::from(self.news_1h)
            + u32::from(self.news_6h)
            + u32::from(self.news_24h)
            + u32::from(self.news_3d)
    }
}

pub(crate) struct SampleSourceData<'a> {
    pub(crate) now: DateTime<Utc>,
    pub(crate) ticker: &'a str,
    pub(crate) ticker_frame: &'a PriceFrame,
    pub(crate) benchmark_frame: &'a PriceFrame,
    pub(crate) qqq_frame: &'a PriceFrame,
    pub(crate) sector_frame: &'a PriceFrame,
    pub(crate) sector_available: bool,
    pub(crate) has_sector_benchmark: bool,
    pub(crate) filings: &'a [SecFiling],
    pub(crate) trade_halts: &'a [NasdaqTradeHalt],
    pub(crate) articles: &'a [GoogleArticle],
    pub(crate) news_source_truncated: bool,
    pub(crate) source_availability: SourceAvailability,
}

pub async fn collect_ml_data(ticker: &str) -> MLData {
    collect_ml_data_now(ticker).await
}

pub async fn collect_ml_data_now(ticker: &str) -> MLData {
    let client = build_client();
    let normalized_ticker = normalize_ticker(ticker);
    let now = Utc::now();
    let sector_benchmark_symbol = lookup_sector_benchmark_symbol(&normalized_ticker, None);
    let halt_date = now.with_timezone(&New_York).date_naive();

    let news_1h_url = build_ticker_news_url(&normalized_ticker, "1h");
    let news_6h_url = build_ticker_news_url(&normalized_ticker, "6h");
    let news_24h_url = build_ticker_news_url(&normalized_ticker, "1d");
    let news_3d_url = build_ticker_news_url(&normalized_ticker, "3d");

    let (
        ticker_frame_result,
        benchmark_frame_result,
        qqq_frame_result,
        sector_frame_result,
        filings_result,
        trade_halts_result,
        news_1h_result,
        news_6h_result,
        news_24h_result,
        news_3d_result,
    ) = tokio::join!(
        fetch_price_frame(&normalized_ticker, &client),
        fetch_price_frame(BENCHMARK_SYMBOL, &client),
        fetch_price_frame(QQQ_SYMBOL, &client),
        fetch_optional_price_frame(sector_benchmark_symbol, &client),
        fetch_sec_edgar_ticker(&normalized_ticker, &client),
        fetch_nasdaq_trade_halts_for_date(&client, halt_date),
        fetch_google_news(&news_1h_url, &client),
        fetch_google_news(&news_6h_url, &client),
        fetch_google_news(&news_24h_url, &client),
        fetch_google_news(&news_3d_url, &client),
    );

    let ticker_frame_available = ticker_frame_result.is_ok();
    let benchmark_frame_available = benchmark_frame_result.is_ok();
    let qqq_frame_available = qqq_frame_result.is_ok();
    let sector_frame_available = matches!(sector_frame_result.as_ref(), Ok(Some(_)));
    let filings_available = filings_result.is_ok();
    let trade_halts_available = trade_halts_result.is_ok();
    let news_1h_available = news_1h_result.is_ok();
    let news_6h_available = news_6h_result.is_ok();
    let news_24h_available = news_24h_result.is_ok();
    let news_3d_available = news_3d_result.is_ok();

    let ticker_frame = ticker_frame_result.unwrap_or_default();
    let benchmark_frame = benchmark_frame_result.unwrap_or_default();
    let qqq_frame = qqq_frame_result.unwrap_or_default();
    let sector_available = sector_frame_available;
    let sector_frame = match sector_frame_result {
        Ok(Some(frame)) => frame,
        _ => PriceFrame::default(),
    };
    let filings = filings_result.unwrap_or_default();
    let trade_halts = trade_halts_result.unwrap_or_default();
    let news_1h = news_1h_result.unwrap_or_default();
    let news_6h = news_6h_result.unwrap_or_default();
    let news_24h = news_24h_result.unwrap_or_default();
    let news_3d = news_3d_result.unwrap_or_default();
    let articles = merged_news_articles(&[&news_1h, &news_6h, &news_24h, &news_3d]);
    let source_availability = SourceAvailability {
        ticker_frame: ticker_frame_available,
        benchmark_frame: benchmark_frame_available,
        qqq_frame: qqq_frame_available,
        sector_frame: sector_frame_available,
        filings: filings_available,
        trade_halts: trade_halts_available,
        news_1h: news_1h_available,
        news_6h: news_6h_available,
        news_24h: news_24h_available,
        news_3d: news_3d_available,
    };

    build_ml_data_from_source_data(&SampleSourceData {
        now,
        ticker: &normalized_ticker,
        ticker_frame: &ticker_frame,
        benchmark_frame: &benchmark_frame,
        qqq_frame: &qqq_frame,
        sector_frame: &sector_frame,
        sector_available,
        has_sector_benchmark: sector_benchmark_symbol.is_some(),
        filings: &filings,
        trade_halts: &trade_halts,
        articles: &articles,
        news_source_truncated: news_24h.len() >= GOOGLE_NEWS_QUERY_ITEM_CAP
            || news_3d.len() >= GOOGLE_NEWS_QUERY_ITEM_CAP,
        source_availability,
    })
}

pub(crate) fn canonical_close_sample_timestamp(date: NaiveDate) -> DateTime<Utc> {
    let local_close = date
        .and_hms_opt(CANONICAL_CLOSE_HOUR_ET, CANONICAL_CLOSE_MINUTE_ET, 0)
        .and_then(|timestamp| New_York.from_local_datetime(&timestamp).single())
        .expect("market-close timestamps should be unambiguous in New York");

    local_close.with_timezone(&Utc)
}

pub(crate) fn close_sample_price_frame(
    symbol: &str,
    history: &[DailyBar],
    current_bar: &DailyBar,
) -> PriceFrame {
    let prev_close = history
        .last()
        .map(|bar| bar.close)
        .filter(|value| *value > 0.0)
        .unwrap_or(current_bar.open.max(current_bar.close));

    PriceFrame {
        symbol: symbol.to_string(),
        snapshot: crate::sources::StockSnapshot {
            latest_price: current_bar.close,
            open: current_bar.open,
            high: current_bar.high,
            low: current_bar.low,
            close: current_bar.close,
            prev_close,
            volume: current_bar.volume,
        },
        bars: history.to_vec(),
    }
}

pub(crate) fn build_ml_data_from_source_data(source: &SampleSourceData<'_>) -> MLData {
    let source_successes = source
        .source_availability
        .success_count(source.has_sector_benchmark);
    let source_targets = 9 + u32::from(source.has_sector_benchmark);
    let filing_features = derive_filing_features(&source.filings, source.now);
    let has_recent_halt = source
        .trade_halts
        .iter()
        .any(|halt| halt.ticker.eq_ignore_ascii_case(&source.ticker));
    let news_features = derive_news_features_from_articles(
        &source.ticker,
        &source.articles,
        filing_features.recent_earnings_filing_within_7d,
        has_recent_halt,
        source.now,
        source.news_source_truncated,
    );

    build_ml_data_sample(
        source.now,
        &source.ticker_frame,
        &source.benchmark_frame,
        &source.qqq_frame,
        &source.sector_frame,
        source.sector_available,
        &source.filings,
        &news_features,
        source.source_availability,
        source_successes,
        source_targets,
        source.has_sector_benchmark,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_ml_data_sample(
    now: DateTime<Utc>,
    ticker_frame: &PriceFrame,
    benchmark_frame: &PriceFrame,
    qqq_frame: &PriceFrame,
    sector_frame: &PriceFrame,
    sector_available: bool,
    filings: &[SecFiling],
    news_features: &NewsFeatures,
    source_availability: SourceAvailability,
    source_successes: u32,
    source_targets: u32,
    has_sector_benchmark: bool,
) -> MLData {
    let price_features = derive_price_features(ticker_frame);
    let benchmark_price_features = derive_price_features(&benchmark_frame);
    let sector_price_features = derive_price_features(&sector_frame);
    let qqq_price_features = derive_price_features(&qqq_frame);
    let relative_features = derive_relative_features(
        &ticker_frame,
        &benchmark_frame,
        &sector_frame,
        sector_available,
        &price_features,
        &benchmark_price_features,
        &sector_price_features,
        &qqq_price_features,
    );
    let filing_features = derive_filing_features(filings, now);
    let (market_regime, regime_confidence) =
        classify_market_regime(&benchmark_price_features, &qqq_price_features);
    let quality = derive_quality_metrics(
        &source_availability,
        source_successes,
        source_targets,
        ticker_frame,
        benchmark_frame,
        qqq_frame,
        has_sector_benchmark,
        sector_frame,
        &filing_features,
        news_features,
        price_features.current_price,
    );
    let shared_context = build_shared_context(now, price_features.current_price, &quality);

    MLData {
        generalist: GeneralistInput {
            ctx: shared_context.clone(),
            return_1d: as_f32(price_features.return_1d),
            return_3d: as_f32(price_features.return_3d),
            return_5d: as_f32(price_features.return_5d),
            return_10d: as_f32(price_features.return_10d),
            return_20d: as_f32(price_features.return_20d),
            sma_10_distance: as_f32(price_features.sma_10_distance),
            sma_20_distance: as_f32(price_features.sma_20_distance),
            sma_50_distance: as_f32(price_features.sma_50_distance),
            sma_20_slope: as_f32(price_features.sma_20_slope),
            sma_50_slope: as_f32(price_features.sma_50_slope),
            drawdown_from_20d_high: as_f32(price_features.drawdown_from_20d_high),
            drawdown_from_60d_high: as_f32(price_features.drawdown_from_60d_high),
            range_position_20d: as_f32(price_features.range_position_20d),
            range_position_60d: as_f32(price_features.range_position_60d),
            realized_vol_5d: as_f32(price_features.realized_vol_5d),
            realized_vol_10d: as_f32(price_features.realized_vol_10d),
            realized_vol_20d: as_f32(price_features.realized_vol_20d),
            atr_14_pct: as_f32(price_features.atr_14_pct),
            volume_vs_5d_avg: as_f32(price_features.volume_vs_5d_avg),
            volume_vs_20d_avg: as_f32(price_features.volume_vs_20d_avg),
            dollar_volume_vs_20d_avg: as_f32(price_features.dollar_volume_vs_20d_avg),
            abnormal_volume_score: as_f32(price_features.abnormal_volume_score),
            benchmark_return_1d: as_f32(relative_features.benchmark_return_1d),
            benchmark_return_5d: as_f32(relative_features.benchmark_return_5d),
            benchmark_return_20d: as_f32(relative_features.benchmark_return_20d),
            sector_return_1d: as_f32(relative_features.sector_return_1d),
            sector_return_5d: as_f32(relative_features.sector_return_5d),
            sector_return_20d: as_f32(relative_features.sector_return_20d),
            excess_return_vs_benchmark_5d: as_f32(relative_features.excess_return_vs_benchmark_5d),
            excess_return_vs_benchmark_20d: as_f32(
                relative_features.excess_return_vs_benchmark_20d,
            ),
            excess_return_vs_sector_5d: as_f32(relative_features.excess_return_vs_sector_5d),
            excess_return_vs_sector_20d: as_f32(relative_features.excess_return_vs_sector_20d),
            rolling_beta_20d: as_f32(relative_features.rolling_beta_20d),
            rolling_corr_benchmark_20d: as_f32(relative_features.rolling_corr_benchmark_20d),
            qqq_return_1d: as_f32(relative_features.qqq_return_1d),
            qqq_return_5d: as_f32(relative_features.qqq_return_5d),
            spy_return_1d: as_f32(relative_features.spy_return_1d),
            spy_return_5d: as_f32(relative_features.spy_return_5d),
            market_regime,
            regime_confidence,
            news_count_24h: news_features.news_count_24h,
            abnormal_news_count_24h: as_f32(news_features.abnormal_news_count_24h),
            avg_news_sentiment_24h: as_f32(news_features.avg_news_sentiment_24h),
            sentiment_change_6h_vs_24h: as_f32(news_features.sentiment_change_6h_vs_24h),
            sentiment_dispersion_24h: as_f32(news_features.sentiment_dispersion_24h),
            has_high_impact_news_24h: news_features.has_high_impact_news_24h,
        },
        technical: TechnicalSpecialistInput {
            ctx: shared_context.clone(),
            return_1d: as_f32(price_features.return_1d),
            return_2d: as_f32(price_features.return_2d),
            return_3d: as_f32(price_features.return_3d),
            return_5d: as_f32(price_features.return_5d),
            return_10d: as_f32(price_features.return_10d),
            return_20d: as_f32(price_features.return_20d),
            return_60d: as_f32(price_features.return_60d),
            intraday_return_today: as_f32(price_features.intraday_return_today),
            overnight_return_today: as_f32(price_features.overnight_return_today),
            gap_from_prev_close: as_f32(price_features.gap_from_prev_close),
            hl_range_1d: as_f32(price_features.hl_range_1d),
            sma_5_distance: as_f32(price_features.sma_5_distance),
            sma_10_distance: as_f32(price_features.sma_10_distance),
            sma_20_distance: as_f32(price_features.sma_20_distance),
            sma_50_distance: as_f32(price_features.sma_50_distance),
            ema_10_distance: as_f32(price_features.ema_10_distance),
            ema_20_distance: as_f32(price_features.ema_20_distance),
            sma_20_slope: as_f32(price_features.sma_20_slope),
            sma_50_slope: as_f32(price_features.sma_50_slope),
            ema_20_slope: as_f32(price_features.ema_20_slope),
            trend_strength_20d: as_f32(price_features.trend_strength_20d),
            price_zscore_10d: as_f32(price_features.price_zscore_10d),
            price_zscore_20d: as_f32(price_features.price_zscore_20d),
            return_zscore_5d: as_f32(price_features.return_zscore_5d),
            return_zscore_20d: as_f32(price_features.return_zscore_20d),
            mean_reversion_score_5d: as_f32(price_features.mean_reversion_score_5d),
            momentum_acceleration_5d_vs_20d: as_f32(price_features.momentum_acceleration_5d_vs_20d),
            distance_to_20d_high: as_f32(price_features.distance_to_20d_high),
            distance_to_60d_high: as_f32(price_features.distance_to_60d_high),
            distance_to_20d_low: as_f32(price_features.distance_to_20d_low),
            distance_to_60d_low: as_f32(price_features.distance_to_60d_low),
            drawdown_from_20d_high: as_f32(price_features.drawdown_from_20d_high),
            drawdown_from_60d_high: as_f32(price_features.drawdown_from_60d_high),
            range_position_20d: as_f32(price_features.range_position_20d),
            range_position_60d: as_f32(price_features.range_position_60d),
            realized_vol_5d: as_f32(price_features.realized_vol_5d),
            realized_vol_10d: as_f32(price_features.realized_vol_10d),
            realized_vol_20d: as_f32(price_features.realized_vol_20d),
            downside_vol_10d: as_f32(price_features.downside_vol_10d),
            upside_vol_10d: as_f32(price_features.upside_vol_10d),
            atr_14_pct: as_f32(price_features.atr_14_pct),
            volume_vs_5d_avg: as_f32(price_features.volume_vs_5d_avg),
            volume_vs_20d_avg: as_f32(price_features.volume_vs_20d_avg),
            abnormal_volume_score: as_f32(price_features.abnormal_volume_score),
            volume_trend_5d: as_f32(price_features.volume_trend_5d),
        },
        earnings: EarningsSpecialistInput {
            ctx: shared_context.clone(),
            days_since_last_earnings: filing_features.days_since_last_earnings,
            recent_earnings_filing_within_7d: filing_features.recent_earnings_filing_within_7d,
            recent_earnings_filing_within_30d: filing_features.recent_earnings_filing_within_30d,
            recent_earnings_filing_count_90d: filing_features.recent_earnings_filing_count_90d,
            latest_earnings_filing_age_hours: filing_features.latest_earnings_filing_age_hours,
            return_3d: as_f32(price_features.return_3d),
            return_5d: as_f32(price_features.return_5d),
            return_10d: as_f32(price_features.return_10d),
            excess_return_vs_benchmark_5d: as_f32(relative_features.excess_return_vs_benchmark_5d),
            realized_vol_5d: as_f32(price_features.realized_vol_5d),
            realized_vol_10d: as_f32(price_features.realized_vol_10d),
            drawdown_from_20d_high: as_f32(price_features.drawdown_from_20d_high),
            volume_vs_5d_avg: as_f32(price_features.volume_vs_5d_avg),
            abnormal_volume_score: as_f32(price_features.abnormal_volume_score),
            news_count_24h: news_features.news_count_24h,
            abnormal_news_count_24h: as_f32(news_features.abnormal_news_count_24h),
            avg_news_sentiment_24h: as_f32(news_features.avg_news_sentiment_24h),
            sentiment_change_6h_vs_24h: as_f32(news_features.sentiment_change_6h_vs_24h),
            sentiment_dispersion_24h: as_f32(news_features.sentiment_dispersion_24h),
            dominant_news_category: news_features.dominant_news_category,
            has_high_impact_news_24h: news_features.has_high_impact_news_24h,
        },
        news_event: NewsEventSpecialistInput {
            ctx: shared_context.clone(),
            news_count_1h: news_features.news_count_1h,
            news_count_6h: news_features.news_count_6h,
            news_count_24h: news_features.news_count_24h,
            news_count_3d: news_features.news_count_3d,
            abnormal_news_count_6h: as_f32(news_features.abnormal_news_count_6h),
            abnormal_news_count_24h: as_f32(news_features.abnormal_news_count_24h),
            avg_news_sentiment_1h: as_f32(news_features.avg_news_sentiment_1h),
            avg_news_sentiment_6h: as_f32(news_features.avg_news_sentiment_6h),
            avg_news_sentiment_24h: as_f32(news_features.avg_news_sentiment_24h),
            sentiment_change_6h_vs_24h: as_f32(news_features.sentiment_change_6h_vs_24h),
            sentiment_dispersion_24h: as_f32(news_features.sentiment_dispersion_24h),
            positive_news_ratio_24h: as_f32(news_features.positive_news_ratio_24h),
            negative_news_ratio_24h: as_f32(news_features.negative_news_ratio_24h),
            relevance_weighted_news_sentiment_24h: as_f32(
                news_features.relevance_weighted_news_sentiment_24h,
            ),
            hours_since_latest_news: news_features.hours_since_latest_news,
            news_novelty_score_24h: news_features.news_novelty_score_24h,
            dominant_news_category: news_features.dominant_news_category,
            has_high_impact_news_24h: news_features.has_high_impact_news_24h,
            return_1d: as_f32(price_features.return_1d),
            return_3d: as_f32(price_features.return_3d),
            realized_vol_5d: as_f32(price_features.realized_vol_5d),
            abnormal_volume_score: as_f32(price_features.abnormal_volume_score),
            excess_return_vs_benchmark_1d: as_f32(relative_features.excess_return_vs_benchmark_1d),
        },
        regime: RegimeSpecialistInput {
            ctx: shared_context.clone(),
            spy_return_1d: as_f32(relative_features.spy_return_1d),
            spy_return_5d: as_f32(relative_features.spy_return_5d),
            qqq_return_1d: as_f32(relative_features.qqq_return_1d),
            qqq_return_5d: as_f32(relative_features.qqq_return_5d),
            benchmark_return_20d: as_f32(relative_features.benchmark_return_20d),
            rolling_beta_20d: as_f32(relative_features.rolling_beta_20d),
            rolling_beta_60d: as_f32(relative_features.rolling_beta_60d),
            rolling_corr_benchmark_20d: as_f32(relative_features.rolling_corr_benchmark_20d),
            idiosyncratic_vol_20d: as_f32(relative_features.idiosyncratic_vol_20d),
            market_regime,
            regime_confidence,
        },
        aggregator: AggregatorInput {
            ctx: shared_context,
            market_regime,
            missing_feature_fraction: quality.missing_feature_fraction,
            data_quality_score: quality.data_quality_score,
        },
    }
}

async fn fetch_optional_price_frame(
    symbol: Option<&'static str>,
    client: &Client,
) -> AppResult<Option<PriceFrame>> {
    match symbol {
        Some(symbol) => fetch_price_frame(symbol, client).await.map(Some),
        None => Ok(None),
    }
}

fn derive_price_features(frame: &PriceFrame) -> PriceFeatures {
    let current_price = frame.snapshot.latest_price;
    let closes = close_series(frame);
    let returns = simple_returns(&closes);
    let current_volume = frame.snapshot.volume;
    let historical_volumes: Vec<f64> = frame.bars.iter().map(|bar| bar.volume).collect();
    let historical_dollar_volumes: Vec<f64> = frame
        .bars
        .iter()
        .map(|bar| bar.close * bar.volume)
        .collect();
    let high_low_bars = combined_bars(frame);

    PriceFeatures {
        current_price,
        return_1d: series_return(&closes, 1),
        return_2d: series_return(&closes, 2),
        return_3d: series_return(&closes, 3),
        return_5d: series_return(&closes, 5),
        return_10d: series_return(&closes, 10),
        return_20d: series_return(&closes, 20),
        return_60d: series_return(&closes, 60),
        intraday_return_today: safe_ratio(frame.snapshot.latest_price, frame.snapshot.open),
        overnight_return_today: safe_ratio(frame.snapshot.open, frame.snapshot.prev_close),
        gap_from_prev_close: safe_ratio(frame.snapshot.open, frame.snapshot.prev_close),
        hl_range_1d: if frame.snapshot.low > 0.0 {
            (frame.snapshot.high - frame.snapshot.low) / frame.snapshot.low
        } else {
            0.0
        },
        sma_5_distance: distance_from_average(&closes, 5),
        sma_10_distance: distance_from_average(&closes, 10),
        sma_20_distance: distance_from_average(&closes, 20),
        sma_50_distance: distance_from_average(&closes, 50),
        ema_10_distance: distance_from_ema(&closes, 10),
        ema_20_distance: distance_from_ema(&closes, 20),
        sma_20_slope: moving_average_slope(&closes, 20),
        sma_50_slope: moving_average_slope(&closes, 50),
        ema_20_slope: ema_slope(&closes, 20),
        trend_strength_20d: trend_strength(series_return(&closes, 20), realized_vol(&closes, 20)),
        price_zscore_10d: zscore_last(&closes, 10),
        price_zscore_20d: zscore_last(&closes, 20),
        return_zscore_5d: zscore_last(&returns, 5),
        return_zscore_20d: zscore_last(&returns, 20),
        mean_reversion_score_5d: -zscore_last(&returns, 5),
        momentum_acceleration_5d_vs_20d: series_return(&closes, 5)
            - (series_return(&closes, 20) / 4.0),
        distance_to_20d_high: distance_to_high(&closes, 20),
        distance_to_60d_high: distance_to_high(&closes, 60),
        distance_to_20d_low: distance_to_low(&closes, 20),
        distance_to_60d_low: distance_to_low(&closes, 60),
        drawdown_from_20d_high: drawdown_from_high(&closes, 20),
        drawdown_from_60d_high: drawdown_from_high(&closes, 60),
        range_position_20d: range_position(&closes, 20),
        range_position_60d: range_position(&closes, 60),
        realized_vol_5d: realized_vol(&closes, 5),
        realized_vol_10d: realized_vol(&closes, 10),
        realized_vol_20d: realized_vol(&closes, 20),
        downside_vol_10d: realized_side_vol(&closes, 10, true),
        upside_vol_10d: realized_side_vol(&closes, 10, false),
        atr_14_pct: atr_pct(&high_low_bars, 14),
        volume_vs_5d_avg: ratio_to_average(current_volume, &historical_volumes, 5),
        volume_vs_20d_avg: ratio_to_average(current_volume, &historical_volumes, 20),
        dollar_volume_vs_20d_avg: ratio_to_average(
            current_price * current_volume,
            &historical_dollar_volumes,
            20,
        ),
        abnormal_volume_score: zscore_value(current_volume, &historical_volumes, 20),
        volume_trend_5d: rolling_average_change(&historical_volumes, 5),
    }
}

#[allow(clippy::too_many_arguments)]
fn derive_relative_features(
    ticker_frame: &PriceFrame,
    benchmark_frame: &PriceFrame,
    sector_frame: &PriceFrame,
    sector_available: bool,
    price_features: &PriceFeatures,
    benchmark_price_features: &PriceFeatures,
    sector_price_features: &PriceFeatures,
    qqq_price_features: &PriceFeatures,
) -> RelativeFeatures {
    let ticker_closes = close_series(ticker_frame);
    let benchmark_closes = close_series(benchmark_frame);
    let _sector_closes = close_series(sector_frame);

    RelativeFeatures {
        benchmark_return_1d: benchmark_price_features.return_1d,
        benchmark_return_5d: benchmark_price_features.return_5d,
        benchmark_return_20d: benchmark_price_features.return_20d,
        sector_return_1d: if sector_available {
            sector_price_features.return_1d
        } else {
            0.0
        },
        sector_return_5d: if sector_available {
            sector_price_features.return_5d
        } else {
            0.0
        },
        sector_return_20d: if sector_available {
            sector_price_features.return_20d
        } else {
            0.0
        },
        excess_return_vs_benchmark_1d: price_features.return_1d
            - benchmark_price_features.return_1d,
        excess_return_vs_benchmark_5d: price_features.return_5d
            - benchmark_price_features.return_5d,
        excess_return_vs_benchmark_20d: price_features.return_20d
            - benchmark_price_features.return_20d,
        excess_return_vs_sector_5d: if sector_available {
            price_features.return_5d - sector_price_features.return_5d
        } else {
            0.0
        },
        excess_return_vs_sector_20d: if sector_available {
            price_features.return_20d - sector_price_features.return_20d
        } else {
            0.0
        },
        rolling_beta_20d: rolling_beta(&ticker_closes, &benchmark_closes, 20),
        rolling_beta_60d: rolling_beta(&ticker_closes, &benchmark_closes, 60),
        rolling_corr_benchmark_20d: rolling_correlation(&ticker_closes, &benchmark_closes, 20),
        idiosyncratic_vol_20d: idiosyncratic_vol(&ticker_closes, &benchmark_closes, 20),
        spy_return_1d: benchmark_price_features.return_1d,
        spy_return_5d: benchmark_price_features.return_5d,
        qqq_return_1d: qqq_price_features.return_1d,
        qqq_return_5d: qqq_price_features.return_5d,
    }
}

fn derive_filing_features(filings: &[SecFiling], now: DateTime<Utc>) -> FilingFeatures {
    let mut earnings_filing_ages_days = Vec::new();
    let mut latest_earnings_filing_age_hours = None;

    for filing in filings.iter().filter(|filing| is_earnings_filing(filing)) {
        let Some(filed_at) = parse_filing_datetime(filing) else {
            continue;
        };
        let Some(age) = non_negative_age(now, filed_at) else {
            continue;
        };

        let age_hours = age.num_seconds() as f32 / 3600.0;
        let age_days = age_hours / 24.0;
        earnings_filing_ages_days.push(age_days);
        latest_earnings_filing_age_hours = match latest_earnings_filing_age_hours {
            Some(existing) if existing <= age_hours => Some(existing),
            _ => Some(age_hours),
        };
    }

    FilingFeatures {
        days_since_last_earnings: earnings_filing_ages_days
            .iter()
            .copied()
            .min_by(|left, right| left.total_cmp(right)),
        recent_earnings_filing_within_7d: earnings_filing_ages_days.iter().any(|days| *days <= 7.0),
        recent_earnings_filing_within_30d: earnings_filing_ages_days
            .iter()
            .any(|days| *days <= 30.0),
        recent_earnings_filing_count_90d: earnings_filing_ages_days
            .iter()
            .filter(|days| **days <= 90.0)
            .count() as u16,
        latest_earnings_filing_age_hours,
    }
}

pub(crate) fn derive_news_features_from_articles(
    ticker: &str,
    articles: &[GoogleArticle],
    has_recent_earnings_filing: bool,
    has_recent_halt: bool,
    now: DateTime<Utc>,
    source_truncated: bool,
) -> NewsFeatures {
    derive_news_features_from_merged_articles(
        ticker,
        &merge_unique_articles(&[articles]),
        has_recent_earnings_filing,
        has_recent_halt,
        now,
        source_truncated,
    )
}

fn derive_news_features_from_merged_articles(
    ticker: &str,
    merged_articles: &[&GoogleArticle],
    has_recent_earnings_filing: bool,
    has_recent_halt: bool,
    now: DateTime<Utc>,
    source_truncated: bool,
) -> NewsFeatures {
    let window_1h = articles_within_hours(merged_articles, now, 1.0);
    let window_6h = articles_within_hours(merged_articles, now, 6.0);
    let window_24h = articles_within_hours(merged_articles, now, 24.0);
    let window_3d = articles_within_hours(merged_articles, now, 72.0);
    let sentiments_24h: Vec<f64> = window_24h
        .iter()
        .map(|article| article_sentiment(article))
        .collect();
    let dominant_news_category = dominant_news_category(&window_24h);
    let avg_news_sentiment_1h = average_article_sentiment_refs(&window_1h);
    let avg_news_sentiment_6h = average_article_sentiment_refs(&window_6h);
    let avg_news_sentiment_24h = average_article_sentiment_refs(&window_24h);
    let has_high_impact_news_24h = has_recent_halt
        || has_recent_earnings_filing
        || window_24h
            .iter()
            .any(|article| is_high_impact_article(article))
        || matches!(
            dominant_news_category,
            NewsCategory::Earnings | NewsCategory::LegalRegulatory | NewsCategory::MAndA
        );

    NewsFeatures {
        news_count_1h: window_1h.len() as u16,
        news_count_6h: window_6h.len() as u16,
        news_count_24h: window_24h.len() as u16,
        news_count_3d: window_3d.len() as u16,
        abnormal_news_count_6h: scaled_news_ratio(window_6h.len(), window_24h.len(), 4.0),
        abnormal_news_count_24h: scaled_news_ratio(window_24h.len(), window_3d.len(), 3.0),
        avg_news_sentiment_1h,
        avg_news_sentiment_6h,
        avg_news_sentiment_24h,
        sentiment_change_6h_vs_24h: avg_news_sentiment_6h - avg_news_sentiment_24h,
        sentiment_dispersion_24h: std_dev(&sentiments_24h),
        positive_news_ratio_24h: sentiment_ratio(&sentiments_24h, true),
        negative_news_ratio_24h: sentiment_ratio(&sentiments_24h, false),
        relevance_weighted_news_sentiment_24h: relevance_weighted_sentiment(ticker, &window_24h),
        hours_since_latest_news: latest_news_age_hours(&merged_articles, now),
        news_novelty_score_24h: news_novelty_score(&window_24h, &window_3d, now, source_truncated),
        dominant_news_category,
        has_high_impact_news_24h,
        source_truncated,
    }
}

fn classify_market_regime(
    benchmark_price_features: &PriceFeatures,
    qqq_price_features: &PriceFeatures,
) -> (MarketRegime, f32) {
    let risk_on_score =
        ((benchmark_price_features.return_5d + qqq_price_features.return_5d) / 2.0) / 0.03;
    let risk_off_score = -risk_on_score;
    let high_vol_score = if benchmark_price_features.realized_vol_20d > 0.0 {
        benchmark_price_features.realized_vol_5d / benchmark_price_features.realized_vol_20d
    } else {
        0.0
    };
    let trend_score = trend_strength(
        benchmark_price_features.return_20d,
        benchmark_price_features.realized_vol_20d,
    );

    if high_vol_score > 1.4 {
        return (
            MarketRegime::HighVol,
            (high_vol_score / 2.0).clamp(0.0, 1.0) as f32,
        );
    }

    if risk_on_score > 1.0 {
        return (
            MarketRegime::RiskOn,
            (risk_on_score / 2.0).clamp(0.0, 1.0) as f32,
        );
    }

    if risk_off_score > 1.0 {
        return (
            MarketRegime::RiskOff,
            (risk_off_score / 2.0).clamp(0.0, 1.0) as f32,
        );
    }

    if trend_score.abs() > 1.25 {
        return (
            MarketRegime::Trend,
            (trend_score.abs() / 2.0).clamp(0.0, 1.0) as f32,
        );
    }

    if high_vol_score > 1.15 {
        return (
            MarketRegime::MeanReversion,
            ((high_vol_score - 1.0) / 0.5).clamp(0.0, 1.0) as f32,
        );
    }

    (MarketRegime::Neutral, 0.35)
}

fn derive_quality_metrics(
    source_availability: &SourceAvailability,
    source_successes: u32,
    source_targets: u32,
    ticker_frame: &PriceFrame,
    benchmark_frame: &PriceFrame,
    qqq_frame: &PriceFrame,
    has_sector_benchmark: bool,
    sector_frame: &PriceFrame,
    filing_features: &FilingFeatures,
    news_features: &NewsFeatures,
    current_price: f64,
) -> QualityMetrics {
    let source_coverage = if source_targets == 0 {
        1.0
    } else {
        source_successes as f32 / source_targets as f32
    };
    let mut history_coverages = vec![
        history_coverage_score(ticker_frame.bars.len(), REQUIRED_HISTORY_BARS),
        history_coverage_score(benchmark_frame.bars.len(), REQUIRED_HISTORY_BARS),
        history_coverage_score(qqq_frame.bars.len(), REQUIRED_HISTORY_BARS),
    ];

    if has_sector_benchmark {
        history_coverages.push(history_coverage_score(
            sector_frame.bars.len(),
            REQUIRED_HISTORY_BARS,
        ));
    }

    let history_coverage = average_f32(&history_coverages);
    let news_stale = news_features
        .hours_since_latest_news
        .map(|hours| hours > 72.0)
        .unwrap_or(false);
    let stale_data_flag =
        source_coverage < 0.75 || history_coverage < 0.75 || news_stale || current_price <= 0.0;
    let truncation_penalty = if news_features.source_truncated {
        NEWS_TRUNCATION_QUALITY_PENALTY
    } else {
        0.0
    };
    let missing_feature_fraction = derive_missing_feature_fraction(
        source_availability,
        has_sector_benchmark,
        filing_features,
        news_features,
    );
    let missingness_penalty = missing_feature_fraction * 0.25;
    let data_quality_score = (source_coverage * 0.65 + history_coverage * 0.35
        - if stale_data_flag { 0.1 } else { 0.0 }
        - truncation_penalty
        - missingness_penalty)
        .clamp(0.0, 1.0);

    QualityMetrics {
        missing_feature_fraction,
        stale_data_flag,
        data_quality_score,
    }
}

fn build_shared_context(
    now: DateTime<Utc>,
    current_price: f64,
    quality: &QualityMetrics,
) -> SharedContext {
    let market_time = now.with_timezone(&New_York);
    let price = if current_price.is_finite() && current_price > 0.0 {
        current_price
    } else {
        0.0
    };

    SharedContext {
        timestamp_utc: now.timestamp(),
        market_session: current_market_session(market_time),
        day_of_week: market_time.weekday().num_days_from_monday() as u8,
        day_of_month: market_time.day() as u8,
        month: market_time.month() as u8,
        week_of_year: market_time.iso_week().week() as u8,
        is_month_end_window: is_month_end_window(market_time.date_naive()),
        is_quarter_end_window: is_quarter_end_window(market_time.date_naive()),
        is_options_expiry_week: is_options_expiry_week(market_time.date_naive()),
        current_price: price as f32,
        log_price: if price > 0.0 { price.ln() as f32 } else { 0.0 },
        missing_feature_fraction: quality.missing_feature_fraction,
        stale_data_flag: quality.stale_data_flag,
        data_quality_score: quality.data_quality_score,
    }
}

pub(crate) fn merged_news_articles(feeds: &[&[GoogleArticle]]) -> Vec<GoogleArticle> {
    merge_unique_articles(feeds).into_iter().cloned().collect()
}

fn build_ticker_news_url(ticker: &str, window: &str) -> String {
    format!(
        "https://news.google.com/rss/search?q={}+when%3A{}&hl=en-US&gl=US&ceid=US%3Aen",
        ticker, window
    )
}

fn close_series(frame: &PriceFrame) -> Vec<f64> {
    let mut closes: Vec<f64> = frame.bars.iter().map(|bar| bar.close).collect();

    if frame.snapshot.latest_price > 0.0 {
        closes.push(frame.snapshot.latest_price);
    }

    closes
}

fn combined_bars(frame: &PriceFrame) -> Vec<DailyBar> {
    let mut bars = frame.bars.clone();

    if frame.snapshot.high > 0.0 && frame.snapshot.low > 0.0 {
        bars.push(DailyBar {
            timestamp: String::new(),
            open: frame.snapshot.open,
            high: frame.snapshot.high,
            low: frame.snapshot.low,
            close: frame.snapshot.close.max(frame.snapshot.latest_price),
            volume: frame.snapshot.volume,
        });
    }

    bars
}

fn simple_returns(series: &[f64]) -> Vec<f64> {
    series
        .windows(2)
        .filter_map(|window| {
            let previous = window[0];
            let current = window[1];
            (previous > 0.0 && current > 0.0).then(|| (current / previous) - 1.0)
        })
        .collect()
}

fn log_returns(series: &[f64]) -> Vec<f64> {
    series
        .windows(2)
        .filter_map(|window| {
            let previous = window[0];
            let current = window[1];
            (previous > 0.0 && current > 0.0).then(|| (current / previous).ln())
        })
        .collect()
}

fn series_return(series: &[f64], periods: usize) -> f64 {
    if series.len() <= periods {
        return 0.0;
    }

    safe_ratio(series[series.len() - 1], series[series.len() - 1 - periods])
}

fn moving_average(values: &[f64], periods: usize) -> Option<f64> {
    if values.len() < periods || periods == 0 {
        return None;
    }

    Some(values[values.len() - periods..].iter().sum::<f64>() / periods as f64)
}

fn distance_from_average(values: &[f64], periods: usize) -> f64 {
    moving_average(values, periods)
        .map(|average| safe_ratio(*values.last().unwrap_or(&0.0), average))
        .unwrap_or(0.0)
}

fn moving_average_slope(values: &[f64], periods: usize) -> f64 {
    if values.len() < periods + 1 {
        return 0.0;
    }

    let current = moving_average(values, periods).unwrap_or(0.0);
    let previous = moving_average(&values[..values.len() - 1], periods).unwrap_or(0.0);
    safe_ratio(current, previous)
}

fn ema(values: &[f64], periods: usize) -> Option<f64> {
    if values.len() < periods || periods == 0 {
        return None;
    }

    let smoothing = 2.0 / (periods as f64 + 1.0);
    let mut ema_value = values[0];

    for value in values.iter().skip(1) {
        ema_value = value * smoothing + ema_value * (1.0 - smoothing);
    }

    Some(ema_value)
}

fn distance_from_ema(values: &[f64], periods: usize) -> f64 {
    ema(values, periods)
        .map(|ema_value| safe_ratio(*values.last().unwrap_or(&0.0), ema_value))
        .unwrap_or(0.0)
}

fn ema_slope(values: &[f64], periods: usize) -> f64 {
    if values.len() < periods + 1 {
        return 0.0;
    }

    let current = ema(values, periods).unwrap_or(0.0);
    let previous = ema(&values[..values.len() - 1], periods).unwrap_or(0.0);
    safe_ratio(current, previous)
}

fn zscore_last(values: &[f64], periods: usize) -> f64 {
    if values.len() < periods || periods < 2 {
        return 0.0;
    }

    let window = &values[values.len() - periods..];
    let mean = window.iter().sum::<f64>() / window.len() as f64;
    let std = std_dev(window);

    if std == 0.0 {
        0.0
    } else {
        (window[window.len() - 1] - mean) / std
    }
}

fn distance_to_high(series: &[f64], periods: usize) -> f64 {
    recent_extreme(series, periods, true)
        .map(|high| safe_ratio(*series.last().unwrap_or(&0.0), high))
        .unwrap_or(0.0)
}

fn distance_to_low(series: &[f64], periods: usize) -> f64 {
    recent_extreme(series, periods, false)
        .map(|low| safe_ratio(*series.last().unwrap_or(&0.0), low))
        .unwrap_or(0.0)
}

fn drawdown_from_high(series: &[f64], periods: usize) -> f64 {
    recent_extreme(series, periods, true)
        .map(|high| {
            let current = *series.last().unwrap_or(&0.0);
            if high > 0.0 {
                (1.0 - (current / high)).clamp(0.0, 1.0)
            } else {
                0.0
            }
        })
        .unwrap_or(0.0)
}

fn range_position(series: &[f64], periods: usize) -> f64 {
    if series.len() < periods || periods == 0 {
        return 0.0;
    }

    let window = &series[series.len() - periods..];
    let low = window
        .iter()
        .fold(f64::INFINITY, |acc, value| acc.min(*value));
    let high = window
        .iter()
        .fold(f64::NEG_INFINITY, |acc, value| acc.max(*value));
    let current = *window.last().unwrap_or(&0.0);

    if !low.is_finite() || !high.is_finite() || high <= low {
        0.0
    } else {
        ((current - low) / (high - low)).clamp(0.0, 1.0)
    }
}

fn recent_extreme(series: &[f64], periods: usize, find_high: bool) -> Option<f64> {
    if series.len() < periods || periods == 0 {
        return None;
    }

    let window = &series[series.len() - periods..];
    Some(if find_high {
        window
            .iter()
            .fold(f64::NEG_INFINITY, |acc, value| acc.max(*value))
    } else {
        window
            .iter()
            .fold(f64::INFINITY, |acc, value| acc.min(*value))
    })
}

fn realized_vol(series: &[f64], periods: usize) -> f64 {
    let returns = log_returns(series);

    if returns.len() < periods {
        return 0.0;
    }

    std_dev(&returns[returns.len() - periods..])
}

fn realized_side_vol(series: &[f64], periods: usize, downside: bool) -> f64 {
    let returns = log_returns(series);

    if returns.len() < periods {
        return 0.0;
    }

    let filtered: Vec<f64> = returns[returns.len() - periods..]
        .iter()
        .copied()
        .filter(|value| if downside { *value < 0.0 } else { *value > 0.0 })
        .collect();

    std_dev(&filtered)
}

fn atr_pct(bars: &[DailyBar], periods: usize) -> f64 {
    if bars.len() < periods + 1 {
        return 0.0;
    }

    let mut true_ranges = Vec::new();

    for window in bars.windows(2) {
        let previous_close = window[0].close;
        let current = &window[1];
        let true_range = (current.high - current.low)
            .max((current.high - previous_close).abs())
            .max((current.low - previous_close).abs());
        true_ranges.push(true_range);
    }

    if true_ranges.len() < periods {
        return 0.0;
    }

    let atr = true_ranges[true_ranges.len() - periods..]
        .iter()
        .sum::<f64>()
        / periods as f64;
    let current_price = bars.last().map(|bar| bar.close).unwrap_or(0.0);

    if current_price > 0.0 {
        atr / current_price
    } else {
        0.0
    }
}

fn ratio_to_average(value: f64, history: &[f64], periods: usize) -> f64 {
    if value <= 0.0 || history.len() < periods || periods == 0 {
        return 0.0;
    }

    let average = history[history.len() - periods..].iter().sum::<f64>() / periods as f64;
    safe_ratio(value, average)
}

fn zscore_value(value: f64, history: &[f64], periods: usize) -> f64 {
    if history.len() < periods || periods < 2 {
        return 0.0;
    }

    let window = &history[history.len() - periods..];
    let mean = window.iter().sum::<f64>() / window.len() as f64;
    let std = std_dev(window);

    if std == 0.0 {
        0.0
    } else {
        (value - mean) / std
    }
}

fn rolling_average_change(history: &[f64], periods: usize) -> f64 {
    if history.len() < periods * 2 || periods == 0 {
        return 0.0;
    }

    let recent = history[history.len() - periods..].iter().sum::<f64>() / periods as f64;
    let previous = history[history.len() - periods * 2..history.len() - periods]
        .iter()
        .sum::<f64>()
        / periods as f64;

    safe_ratio(recent, previous)
}

fn rolling_beta(lhs_series: &[f64], rhs_series: &[f64], periods: usize) -> f64 {
    let (lhs, rhs) = aligned_return_windows(lhs_series, rhs_series, periods);

    if lhs.len() < 2 || rhs.len() < 2 {
        return 0.0;
    }

    let lhs_mean = lhs.iter().sum::<f64>() / lhs.len() as f64;
    let rhs_mean = rhs.iter().sum::<f64>() / rhs.len() as f64;
    let covariance = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(left, right)| (left - lhs_mean) * (right - rhs_mean))
        .sum::<f64>()
        / (lhs.len() as f64 - 1.0);
    let variance = rhs
        .iter()
        .map(|value| (value - rhs_mean).powi(2))
        .sum::<f64>()
        / (rhs.len() as f64 - 1.0);

    if variance == 0.0 {
        0.0
    } else {
        covariance / variance
    }
}

fn rolling_correlation(lhs_series: &[f64], rhs_series: &[f64], periods: usize) -> f64 {
    let (lhs, rhs) = aligned_return_windows(lhs_series, rhs_series, periods);

    if lhs.len() < 2 || rhs.len() < 2 {
        return 0.0;
    }

    let lhs_std = std_dev(&lhs);
    let rhs_std = std_dev(&rhs);

    if lhs_std == 0.0 || rhs_std == 0.0 {
        return 0.0;
    }

    let lhs_mean = lhs.iter().sum::<f64>() / lhs.len() as f64;
    let rhs_mean = rhs.iter().sum::<f64>() / rhs.len() as f64;
    let covariance = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(left, right)| (left - lhs_mean) * (right - rhs_mean))
        .sum::<f64>()
        / (lhs.len() as f64 - 1.0);

    covariance / (lhs_std * rhs_std)
}

fn idiosyncratic_vol(lhs_series: &[f64], rhs_series: &[f64], periods: usize) -> f64 {
    let (lhs, rhs) = aligned_return_windows(lhs_series, rhs_series, periods);

    if lhs.len() < 2 || rhs.len() < 2 {
        return 0.0;
    }

    let beta = rolling_beta(lhs_series, rhs_series, periods);
    let residuals: Vec<f64> = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(left, right)| left - beta * right)
        .collect();

    std_dev(&residuals)
}

fn aligned_return_windows(
    lhs_series: &[f64],
    rhs_series: &[f64],
    periods: usize,
) -> (Vec<f64>, Vec<f64>) {
    let lhs = simple_returns(lhs_series);
    let rhs = simple_returns(rhs_series);
    let length = lhs.len().min(rhs.len()).min(periods);

    if length == 0 {
        return (Vec::new(), Vec::new());
    }

    (
        lhs[lhs.len() - length..].to_vec(),
        rhs[rhs.len() - length..].to_vec(),
    )
}

fn trend_strength(return_20d: f64, realized_vol_20d: f64) -> f64 {
    if realized_vol_20d <= 0.0 {
        return 0.0;
    }

    return_20d / (realized_vol_20d * (20.0f64).sqrt())
}

fn merge_unique_articles<'a>(feeds: &[&'a [GoogleArticle]]) -> Vec<&'a GoogleArticle> {
    let mut unique_by_headline = HashMap::new();

    for feed in feeds {
        for article in *feed {
            let key = normalize_headline_key(&article.title);

            match unique_by_headline.get(&key).copied() {
                Some(existing) if !article_is_newer(article, existing) => {}
                _ => {
                    unique_by_headline.insert(key, article);
                }
            }
        }
    }

    let mut unique: Vec<&GoogleArticle> = unique_by_headline.into_values().collect();
    unique.sort_by(|left, right| {
        article_timestamp(right)
            .cmp(&article_timestamp(left))
            .then_with(|| left.title.cmp(&right.title))
    });
    unique
}

fn article_is_newer(candidate: &GoogleArticle, existing: &GoogleArticle) -> bool {
    match (article_timestamp(candidate), article_timestamp(existing)) {
        (Some(candidate_time), Some(existing_time)) => candidate_time > existing_time,
        (Some(_), None) => true,
        _ => false,
    }
}

fn article_timestamp(article: &GoogleArticle) -> Option<DateTime<Utc>> {
    article.pub_date.as_deref().and_then(parse_datetime_to_utc)
}

fn article_age_hours(article: &GoogleArticle, now: DateTime<Utc>) -> Option<f32> {
    article_timestamp(article)
        .and_then(|published_at| non_negative_age(now, published_at))
        .map(|age| age.num_seconds() as f32 / 3600.0)
}

fn articles_within_hours<'a>(
    articles: &'a [&'a GoogleArticle],
    now: DateTime<Utc>,
    max_age_hours: f32,
) -> Vec<&'a GoogleArticle> {
    articles
        .iter()
        .copied()
        .filter(|article| {
            article_age_hours(article, now)
                .map(|age| age <= max_age_hours)
                .unwrap_or(false)
        })
        .collect()
}

fn average_article_sentiment_refs(articles: &[&GoogleArticle]) -> f64 {
    if articles.is_empty() {
        return 0.0;
    }

    articles
        .iter()
        .map(|article| article_sentiment(article))
        .sum::<f64>()
        / articles.len() as f64
}

fn dominant_news_category(articles: &[&GoogleArticle]) -> NewsCategory {
    let mut counts = [0_u32; 7];
    let mut other_count = 0_u32;

    for article in articles {
        match categorize_news(article) {
            NewsCategory::Earnings => counts[0] += 1,
            NewsCategory::AnalystAction => counts[1] += 1,
            NewsCategory::Product => counts[2] += 1,
            NewsCategory::LegalRegulatory => counts[3] += 1,
            NewsCategory::Management => counts[4] += 1,
            NewsCategory::Macro => counts[5] += 1,
            NewsCategory::MAndA => counts[6] += 1,
            NewsCategory::Other => other_count += 1,
            NewsCategory::None => {}
        }
    }

    let best = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| **count)
        .map(|(index, count)| (index, *count))
        .unwrap_or((usize::MAX, 0));

    if best.1 > 0 {
        return match best.0 {
            0 => NewsCategory::Earnings,
            1 => NewsCategory::AnalystAction,
            2 => NewsCategory::Product,
            3 => NewsCategory::LegalRegulatory,
            4 => NewsCategory::Management,
            5 => NewsCategory::Macro,
            6 => NewsCategory::MAndA,
            _ => NewsCategory::Other,
        };
    }

    if other_count > 0 {
        NewsCategory::Other
    } else {
        NewsCategory::None
    }
}

fn categorize_news(article: &GoogleArticle) -> NewsCategory {
    let text = format!(
        "{} {}",
        article.title.to_ascii_lowercase(),
        article
            .description
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
    );

    if contains_any(
        &text,
        &[
            "merger",
            "acquisition",
            "acquire",
            "buyout",
            "takeover",
            "deal",
        ],
    ) {
        return NewsCategory::MAndA;
    }

    if contains_any(
        &text,
        &[
            "earnings",
            "eps",
            "revenue",
            "sales",
            "guidance",
            "quarterly",
            "quarter",
            "10-q",
            "10-k",
        ],
    ) {
        return NewsCategory::Earnings;
    }

    if contains_any(
        &text,
        &[
            "upgrade",
            "downgrade",
            "price target",
            "analyst",
            "buy rating",
            "sell rating",
            "overweight",
            "underweight",
        ],
    ) {
        return NewsCategory::AnalystAction;
    }

    if contains_any(
        &text,
        &[
            "sec",
            "lawsuit",
            "court",
            "investigation",
            "antitrust",
            "regulatory",
            "doj",
            "fda",
            "probe",
        ],
    ) {
        return NewsCategory::LegalRegulatory;
    }

    if contains_any(
        &text,
        &[
            "ceo",
            "cfo",
            "chair",
            "appoints",
            "appointment",
            "steps down",
            "resigns",
        ],
    ) {
        return NewsCategory::Management;
    }

    if contains_any(
        &text,
        &[
            "launch",
            "product",
            "device",
            "model",
            "drug",
            "trial",
            "approval",
            "shipments",
        ],
    ) {
        return NewsCategory::Product;
    }

    if contains_any(
        &text,
        &[
            "fed",
            "inflation",
            "cpi",
            "jobs",
            "economy",
            "interest rate",
            "treasury",
            "tariff",
            "recession",
        ],
    ) {
        return NewsCategory::Macro;
    }

    NewsCategory::Other
}

fn is_high_impact_article(article: &GoogleArticle) -> bool {
    let category = categorize_news(article);
    let sentiment = article_sentiment(article).abs();

    matches!(
        category,
        NewsCategory::Earnings
            | NewsCategory::AnalystAction
            | NewsCategory::LegalRegulatory
            | NewsCategory::MAndA
    ) || sentiment >= 0.75
}

fn relevance_weighted_sentiment(ticker: &str, articles: &[&GoogleArticle]) -> f64 {
    let ticker = ticker.to_ascii_lowercase();
    let mut weighted_total = 0.0;
    let mut total_weight = 0.0;

    for article in articles {
        let title = article.title.to_ascii_lowercase();
        let description = article
            .description
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        let mut weight = 1.0;

        if title.contains(&ticker) {
            weight += 0.75;
        }
        if description.contains(&ticker) {
            weight += 0.35;
        }

        weighted_total += article_sentiment(article) * weight;
        total_weight += weight;
    }

    if total_weight > 0.0 {
        weighted_total / total_weight
    } else {
        0.0
    }
}

fn latest_news_age_hours(articles: &[&GoogleArticle], now: DateTime<Utc>) -> Option<f32> {
    articles
        .iter()
        .filter_map(|article| article_age_hours(article, now))
        .min_by(|left, right| left.total_cmp(right))
}

fn news_novelty_score(
    articles_24h: &[&GoogleArticle],
    articles_3d: &[&GoogleArticle],
    now: DateTime<Utc>,
    source_truncated: bool,
) -> Option<f32> {
    if articles_24h.is_empty() || source_truncated {
        return None;
    }

    let historical_headlines: HashSet<String> = articles_3d
        .iter()
        .filter(|article| {
            article_age_hours(article, now)
                .map(|age| age > 24.0)
                .unwrap_or(false)
        })
        .map(|article| normalize_headline_key(&article.title))
        .collect();

    if historical_headlines.is_empty() {
        return None;
    }

    let novel_articles = articles_24h
        .iter()
        .filter(|article| !historical_headlines.contains(&normalize_headline_key(&article.title)))
        .count();

    Some((novel_articles as f32 / articles_24h.len() as f32).clamp(0.0, 1.0))
}

fn derive_missing_feature_fraction(
    source_availability: &SourceAvailability,
    _has_sector_benchmark: bool,
    filing_features: &FilingFeatures,
    news_features: &NewsFeatures,
) -> f32 {
    let tracked_components = vec![
        source_availability.ticker_frame,
        source_availability.benchmark_frame,
        source_availability.qqq_frame,
        source_availability.sector_frame,
        source_availability.filings,
        source_availability.news_available(),
        filing_features.days_since_last_earnings.is_some(),
        filing_features.latest_earnings_filing_age_hours.is_some(),
        news_features.hours_since_latest_news.is_some(),
        news_features.news_novelty_score_24h.is_some(),
    ];

    let missing_components = tracked_components
        .iter()
        .filter(|is_available| !**is_available)
        .count();

    if tracked_components.is_empty() {
        0.0
    } else {
        missing_components as f32 / tracked_components.len() as f32
    }
}

fn scaled_news_ratio(short_count: usize, long_count: usize, scaling_factor: f64) -> f64 {
    if long_count == 0 {
        return 0.0;
    }

    ((short_count as f64 * scaling_factor) / long_count as f64) - 1.0
}

fn sentiment_ratio(sentiments: &[f64], positive: bool) -> f64 {
    if sentiments.is_empty() {
        return 0.0;
    }

    let count = sentiments
        .iter()
        .filter(|value| {
            if positive {
                **value > 0.2
            } else {
                **value < -0.2
            }
        })
        .count();

    count as f64 / sentiments.len() as f64
}

fn normalize_headline_key(title: &str) -> String {
    title
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_earnings_filing(filing: &SecFiling) -> bool {
    matches!(
        filing.form.as_str(),
        "10-Q" | "10-Q/A" | "10-K" | "10-K/A" | "20-F" | "20-F/A"
    ) || filing
        .items
        .as_deref()
        .map(|items| items.contains("2.02"))
        .unwrap_or(false)
        || filing
            .primary_doc_description
            .as_deref()
            .map(|description| {
                contains_any(
                    &description.to_ascii_lowercase(),
                    &[
                        "results",
                        "earnings",
                        "interim report",
                        "quarterly report",
                        "annual report",
                        "half-year",
                        "half year",
                    ],
                )
            })
            .unwrap_or(false)
}

fn contains_any(text: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|pattern| text.contains(pattern))
}

fn current_market_session(now: DateTime<chrono_tz::Tz>) -> MarketSession {
    if matches!(now.weekday(), Weekday::Sat | Weekday::Sun) {
        return MarketSession::Closed;
    }

    let minutes = now.hour() * 60 + now.minute();

    match minutes {
        240..570 => MarketSession::PreMarket,
        570..960 => MarketSession::Regular,
        960..1200 => MarketSession::AfterHours,
        _ => MarketSession::Closed,
    }
}

fn is_month_end_window(date: NaiveDate) -> bool {
    let last_day = last_day_of_month(date.year(), date.month());
    date.day() + 2 >= last_day
}

fn is_quarter_end_window(date: NaiveDate) -> bool {
    matches!(date.month(), 3 | 6 | 9 | 12) && {
        let last_day = last_day_of_month(date.year(), date.month());
        date.day() + 4 >= last_day
    }
}

fn is_options_expiry_week(date: NaiveDate) -> bool {
    let third_friday = third_friday(date.year(), date.month());
    date.iso_week() == third_friday.iso_week()
}

fn third_friday(year: i32, month: u32) -> NaiveDate {
    let mut date = NaiveDate::from_ymd_opt(year, month, 1).unwrap_or(NaiveDate::MIN);

    while date.weekday() != Weekday::Fri {
        date = date
            .checked_add_signed(Duration::days(1))
            .unwrap_or(NaiveDate::MIN);
    }

    date.checked_add_signed(Duration::days(14)).unwrap_or(date)
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };

    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|date| date.checked_sub_signed(Duration::days(1)))
        .map(|date| date.day())
        .unwrap_or(28)
}

fn history_coverage_score(actual_bars: usize, required_bars: usize) -> f32 {
    if required_bars == 0 {
        return 1.0;
    }

    (actual_bars as f32 / required_bars as f32).clamp(0.0, 1.0)
}

fn average_f32(values: &[f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }

    values.iter().sum::<f32>() / values.len() as f32
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() as f64 - 1.0);

    variance.sqrt()
}

fn safe_ratio(current: f64, base: f64) -> f64 {
    if current > 0.0 && base > 0.0 {
        (current / base) - 1.0
    } else {
        0.0
    }
}

fn non_negative_age(now: DateTime<Utc>, value: DateTime<Utc>) -> Option<chrono::Duration> {
    let age = now.signed_duration_since(value);
    (age.num_seconds() >= 0).then_some(age)
}

fn as_f32(value: f64) -> f32 {
    if value.is_finite() { value as f32 } else { 0.0 }
}
