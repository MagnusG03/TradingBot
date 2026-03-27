from handle_data import prepare_model_inputs, build_aggregator_inputs
from model import generalist, technical, earnings, news, regime, aggregation


sample_record = {
    "generalist": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
        "market_regime": "Neutral",
        "regime_confidence": 0.8,
    },
    "technical": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
    },
    "earnings": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
        "dominant_news_category": "Other",
    },
    "news_event": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
        "dominant_news_category": "Other",
    },
    "regime": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
        "market_regime": "Neutral",
        "regime_confidence": 0.8,
    },
    "aggregator": {
        "ctx": {
            "market_session": "Regular",
            "day_of_week": 5,
            "day_of_month": 27,
            "month": 3,
            "week_of_year": 12,
            "is_month_end_window": False,
            "is_quarter_end_window": False,
            "is_options_expiry_week": False,
            "current_price": 175.0,
            "log_price": 5.16,
            "missing_feature_fraction": 0.05,
            "stale_data_flag": False,
            "data_quality_score": 0.95,
        },
        "market_regime": "Neutral",
        "missing_feature_fraction": 0.05,
        "data_quality_score": 0.95,
    },
}

inputs = prepare_model_inputs(sample_record)

generalist_out = generalist.predict(inputs["generalist"], verbose=0)
technical_out = technical.predict(inputs["technical"], verbose=0)
earnings_out = earnings.predict(inputs["earnings"], verbose=0)
news_out = news.predict(inputs["news"], verbose=0)
regime_out = regime.predict(inputs["regime"], verbose=0)

aggregator_inputs = build_aggregator_inputs(
    sample_record,
    generalist_out,
    technical_out,
    earnings_out,
    news_out,
    regime_out,
)

final_out = aggregation.predict(aggregator_inputs, verbose=0)

print("Generalist:", generalist_out)
print("Technical:", technical_out)
print("Earnings:", earnings_out)
print("News:", news_out)
print("Regime:", regime_out)
print("Aggregation:", final_out)
