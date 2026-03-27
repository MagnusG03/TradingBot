from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np


MARKET_SESSION_ORDER = ("PreMarket", "Regular", "AfterHours", "Closed", "Unknown")
MARKET_REGIME_ORDER = (
    "RiskOn",
    "RiskOff",
    "Neutral",
    "HighVol",
    "Trend",
    "MeanReversion",
    "Unknown",
)
NEWS_CATEGORY_ORDER = (
    "None",
    "Earnings",
    "AnalystAction",
    "Product",
    "LegalRegulatory",
    "Management",
    "Macro",
    "MAndA",
    "Other",
    "Unknown",
)

DEFAULT_LOG_PRICE = float(np.log(100.0))
DEFAULT_MISSING_FEATURE_FRACTION = 0.5
DEFAULT_DATA_QUALITY_SCORE = 0.5
DEFAULT_DAYS_SINCE_LAST_EARNINGS = 365.0
DEFAULT_LATEST_EARNINGS_FILING_AGE_HOURS = 24.0 * 90.0
DEFAULT_HOURS_SINCE_LATEST_NEWS = 24.0 * 7.0

BOOL_WITH_AVAILABILITY_FIELDS = {
    "recent_earnings_filing_within_7d",
    "recent_earnings_filing_within_30d",
    "has_high_impact_news_24h",
}

VALUE_WITH_AVAILABILITY_DEFAULTS = {
    "hl_range_1d": 0.0,
    "drawdown_from_20d_high": 0.0,
    "drawdown_from_60d_high": 0.0,
    "distance_to_20d_high": 0.0,
    "distance_to_60d_high": 0.0,
    "distance_to_20d_low": 0.0,
    "distance_to_60d_low": 0.0,
    "range_position_20d": 0.5,
    "range_position_60d": 0.5,
    "realized_vol_5d": 0.0,
    "realized_vol_10d": 0.0,
    "realized_vol_20d": 0.0,
    "downside_vol_10d": 0.0,
    "upside_vol_10d": 0.0,
    "atr_14_pct": 0.0,
    "rolling_beta_20d": 1.0,
    "rolling_beta_60d": 1.0,
    "rolling_corr_benchmark_20d": 0.0,
    "idiosyncratic_vol_20d": 0.0,
    "days_since_last_earnings": DEFAULT_DAYS_SINCE_LAST_EARNINGS,
    "recent_earnings_filing_count_90d": 0.0,
    "latest_earnings_filing_age_hours": DEFAULT_LATEST_EARNINGS_FILING_AGE_HOURS,
    "news_count_1h": 0.0,
    "news_count_6h": 0.0,
    "news_count_24h": 0.0,
    "news_count_3d": 0.0,
    "sentiment_dispersion_24h": 0.0,
    "positive_news_ratio_24h": 0.0,
    "negative_news_ratio_24h": 0.0,
    "hours_since_latest_news": DEFAULT_HOURS_SINCE_LATEST_NEWS,
    "news_novelty_score_24h": 0.0,
}

GENERALIST_FIELDS = (
    "return_1d",
    "return_3d",
    "return_5d",
    "return_10d",
    "return_20d",
    "sma_10_distance",
    "sma_20_distance",
    "sma_50_distance",
    "sma_20_slope",
    "sma_50_slope",
    "drawdown_from_20d_high",
    "drawdown_from_60d_high",
    "range_position_20d",
    "range_position_60d",
    "realized_vol_5d",
    "realized_vol_10d",
    "realized_vol_20d",
    "atr_14_pct",
    "volume_vs_5d_avg",
    "volume_vs_20d_avg",
    "dollar_volume_vs_20d_avg",
    "abnormal_volume_score",
    "benchmark_return_1d",
    "benchmark_return_5d",
    "benchmark_return_20d",
    "sector_return_1d",
    "sector_return_5d",
    "sector_return_20d",
    "excess_return_vs_benchmark_5d",
    "excess_return_vs_benchmark_20d",
    "excess_return_vs_sector_5d",
    "excess_return_vs_sector_20d",
    "rolling_beta_20d",
    "rolling_corr_benchmark_20d",
    "qqq_return_1d",
    "qqq_return_5d",
    "spy_return_1d",
    "spy_return_5d",
    "market_regime",
    "regime_confidence",
    "news_count_24h",
    "abnormal_news_count_24h",
    "avg_news_sentiment_24h",
    "sentiment_change_6h_vs_24h",
    "sentiment_dispersion_24h",
    "has_high_impact_news_24h",
)

TECHNICAL_FIELDS = (
    "return_1d",
    "return_2d",
    "return_3d",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "intraday_return_today",
    "overnight_return_today",
    "gap_from_prev_close",
    "hl_range_1d",
    "sma_5_distance",
    "sma_10_distance",
    "sma_20_distance",
    "sma_50_distance",
    "ema_10_distance",
    "ema_20_distance",
    "sma_20_slope",
    "sma_50_slope",
    "ema_20_slope",
    "trend_strength_20d",
    "price_zscore_10d",
    "price_zscore_20d",
    "return_zscore_5d",
    "return_zscore_20d",
    "mean_reversion_score_5d",
    "momentum_acceleration_5d_vs_20d",
    "distance_to_20d_high",
    "distance_to_60d_high",
    "distance_to_20d_low",
    "distance_to_60d_low",
    "drawdown_from_20d_high",
    "drawdown_from_60d_high",
    "range_position_20d",
    "range_position_60d",
    "realized_vol_5d",
    "realized_vol_10d",
    "realized_vol_20d",
    "downside_vol_10d",
    "upside_vol_10d",
    "atr_14_pct",
    "volume_vs_5d_avg",
    "volume_vs_20d_avg",
    "abnormal_volume_score",
    "volume_trend_5d",
)

EARNINGS_FIELDS = (
    "days_since_last_earnings",
    "recent_earnings_filing_within_7d",
    "recent_earnings_filing_within_30d",
    "recent_earnings_filing_count_90d",
    "latest_earnings_filing_age_hours",
    "return_3d",
    "return_5d",
    "return_10d",
    "excess_return_vs_benchmark_5d",
    "realized_vol_5d",
    "realized_vol_10d",
    "drawdown_from_20d_high",
    "volume_vs_5d_avg",
    "abnormal_volume_score",
    "news_count_24h",
    "abnormal_news_count_24h",
    "avg_news_sentiment_24h",
    "sentiment_change_6h_vs_24h",
    "sentiment_dispersion_24h",
    "dominant_news_category",
    "has_high_impact_news_24h",
)

NEWS_FIELDS = (
    "news_count_1h",
    "news_count_6h",
    "news_count_24h",
    "news_count_3d",
    "abnormal_news_count_6h",
    "abnormal_news_count_24h",
    "avg_news_sentiment_1h",
    "avg_news_sentiment_6h",
    "avg_news_sentiment_24h",
    "sentiment_change_6h_vs_24h",
    "sentiment_dispersion_24h",
    "positive_news_ratio_24h",
    "negative_news_ratio_24h",
    "relevance_weighted_news_sentiment_24h",
    "hours_since_latest_news",
    "news_novelty_score_24h",
    "dominant_news_category",
    "has_high_impact_news_24h",
    "return_1d",
    "return_3d",
    "realized_vol_5d",
    "abnormal_volume_score",
)

REGIME_FIELDS = (
    "spy_return_1d",
    "spy_return_5d",
    "qqq_return_1d",
    "qqq_return_5d",
    "benchmark_return_20d",
    "rolling_beta_20d",
    "rolling_beta_60d",
    "rolling_corr_benchmark_20d",
    "idiosyncratic_vol_20d",
    "market_regime",
    "regime_confidence",
)

AGGREGATOR_META_FIELDS = (
    "market_regime",
    "missing_feature_fraction",
    "data_quality_score",
)


def load_collection_data(source: Any) -> dict[str, Any] | list[dict[str, Any]]:
    if isinstance(source, Mapping):
        return _to_plain_data(source)

    if isinstance(source, (list, tuple)):
        return _to_plain_data(list(source))

    if hasattr(source, "__dict__"):
        return _to_plain_data(source)

    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                return _to_plain_data(json.load(handle))
        return _to_plain_data(json.loads(str(source)))

    raise TypeError("Expected a mapping, object, JSON string, or JSON file path.")


def prepare_model_inputs(source: Any) -> dict[str, np.ndarray]:
    data = load_collection_data(source)
    records = data if isinstance(data, list) else [data]
    vectors = [vectorize_record(record) for record in records]

    return {
        "generalist": _stack(v["generalist"] for v in vectors),
        "technical": _stack(v["technical"] for v in vectors),
        "earnings": _stack(v["earnings"] for v in vectors),
        "news": _stack(v["news"] for v in vectors),
        "regime": _stack(v["regime"] for v in vectors),
        "aggregator_meta": _stack(v["aggregator_meta"] for v in vectors),
    }


def vectorize_record(record: Mapping[str, Any]) -> dict[str, np.ndarray]:
    item = _to_plain_data(record)

    generalist = item["generalist"]
    technical = item["technical"]
    earnings = item["earnings"]
    news = item["news_event"]
    regime = item["regime"]
    aggregator = item["aggregator"]

    generalist_vector = _feature_vector(generalist, GENERALIST_FIELDS, _shared_context_14)
    technical_vector = _feature_vector(technical, TECHNICAL_FIELDS, _shared_context_15)
    earnings_vector = _feature_vector(earnings, EARNINGS_FIELDS, _shared_context_14)
    news_vector = _feature_vector(news, NEWS_FIELDS, _shared_context_15)
    regime_vector = _feature_vector(regime, REGIME_FIELDS, _shared_context_14)
    aggregator_meta_vector = _array(
        _shared_context_14(aggregator.get("ctx", {}))
        + [
            value
            for name in AGGREGATOR_META_FIELDS
            for value in _encode_feature(aggregator, name)
        ],
    )

    _assert_lengths(
        generalist=generalist_vector,
        technical=technical_vector,
        earnings=earnings_vector,
        news=news_vector,
        regime=regime_vector,
        aggregator_meta=aggregator_meta_vector,
    )

    return {
        "generalist": generalist_vector,
        "technical": technical_vector,
        "earnings": earnings_vector,
        "news": news_vector,
        "regime": regime_vector,
        "aggregator_meta": aggregator_meta_vector,
    }


def build_aggregator_inputs(
    source: Any,
    generalist_output: Any,
    technical_output: Any,
    earnings_output: Any,
    news_output: Any,
    regime_output: Any,
) -> list[np.ndarray]:
    prepared = prepare_model_inputs(source)
    batch_size = prepared["aggregator_meta"].shape[0]
    return [
        prepared["aggregator_meta"],
        _prediction_matrix(generalist_output, 2, batch_size),
        _prediction_matrix(technical_output, 2, batch_size),
        _prediction_matrix(earnings_output, 2, batch_size),
        _prediction_matrix(news_output, 3, batch_size),
        _prediction_matrix(regime_output, 2, batch_size),
    ]


def _feature_vector(
    section: Mapping[str, Any],
    feature_names: Iterable[str],
    context_builder: Any,
) -> np.ndarray:
    context = context_builder(section.get("ctx", {}))
    features: list[float] = []
    for name in feature_names:
        features.extend(_encode_feature(section, name))
    return _array(context + features)


def _shared_context_14(ctx: Mapping[str, Any]) -> list[float]:
    return (
        _session_one_hot(ctx.get("market_session"))
        + _cyclical_feature(ctx.get("day_of_week"), 7.0)
        + _cyclical_feature(ctx.get("month"), 12.0, one_based=True)
        + _cyclical_feature(ctx.get("week_of_year"), 53.0, one_based=True)
        + _bool_feature(ctx.get("is_month_end_window"), include_availability=True)
        + _bool_feature(ctx.get("is_quarter_end_window"), include_availability=True)
        + _bool_feature(ctx.get("is_options_expiry_week"), include_availability=True)
        + _numeric_feature(
            ctx.get("log_price"),
            default=DEFAULT_LOG_PRICE,
            include_availability=True,
        )
        + _numeric_feature(
            ctx.get("missing_feature_fraction"),
            default=DEFAULT_MISSING_FEATURE_FRACTION,
            include_availability=True,
        )
        + _bool_feature(ctx.get("stale_data_flag"), include_availability=True)
        + _numeric_feature(
            ctx.get("data_quality_score"),
            default=DEFAULT_DATA_QUALITY_SCORE,
            include_availability=True,
        )
    )


def _shared_context_15(ctx: Mapping[str, Any]) -> list[float]:
    return _shared_context_14(ctx) + _cyclical_feature(
        ctx.get("day_of_month"),
        31.0,
        one_based=True,
    )


def _session_one_hot(value: Any) -> list[float]:
    return _one_hot(
        _enum_name(value),
        MARKET_SESSION_ORDER,
        unknown_label="Unknown",
    )


def _encode_feature(section: Mapping[str, Any], name: str) -> list[float]:
    if name == "market_regime":
        return _one_hot(
            _enum_name(section.get(name)),
            MARKET_REGIME_ORDER,
            unknown_label="Unknown",
        )

    if name == "dominant_news_category":
        return _one_hot(
            _dominant_news_category_name(section),
            NEWS_CATEGORY_ORDER,
            unknown_label="Unknown",
        )

    if name in BOOL_WITH_AVAILABILITY_FIELDS:
        return _bool_feature(section.get(name), include_availability=True)

    if name in VALUE_WITH_AVAILABILITY_DEFAULTS:
        return _numeric_feature(
            section.get(name),
            default=VALUE_WITH_AVAILABILITY_DEFAULTS[name],
            include_availability=True,
        )

    value = section.get(name)
    if isinstance(value, bool):
        return _bool_feature(value)

    return _numeric_feature(value)


def _one_hot(value: str, order: tuple[str, ...], unknown_label: str) -> list[float]:
    normalized = value if value in order else unknown_label
    return [1.0 if normalized == item else 0.0 for item in order]


def _dominant_news_category_name(section: Mapping[str, Any]) -> str:
    normalized = _enum_name(section.get("dominant_news_category"))
    if normalized in NEWS_CATEGORY_ORDER:
        return normalized
    return "None" if _has_explicit_no_news(section) else "Unknown"


def _has_explicit_no_news(section: Mapping[str, Any]) -> bool:
    news_count_fields = (
        "news_count_1h",
        "news_count_6h",
        "news_count_24h",
        "news_count_3d",
    )
    present_values = [
        section.get(name)
        for name in news_count_fields
        if section.get(name) is not None
    ]
    return bool(present_values) and all(_float(value) == 0.0 for value in present_values)


def _cyclical_feature(value: Any, period: float, one_based: bool = False) -> list[float]:
    if period <= 0:
        return [0.0, 0.0, 0.0]

    numeric = _maybe_float(value)
    if numeric is None:
        return [0.0, 0.0, 0.0]

    if one_based:
        numeric -= 1.0

    angle = 2.0 * float(np.pi) * (numeric % period) / period
    return [float(np.sin(angle)), float(np.cos(angle)), 1.0]


def _numeric_feature(
    value: Any,
    default: float = 0.0,
    include_availability: bool = False,
) -> list[float]:
    numeric = _maybe_float(value)
    present = numeric is not None
    encoded = [numeric if present else default]
    if include_availability:
        encoded.append(1.0 if present else 0.0)
    return encoded


def _float(value: Any) -> float:
    numeric = _maybe_float(value)
    if numeric is not None:
        return numeric
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return 0.0


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if np.isfinite(numeric) else None
    if isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            return None
        return numeric if np.isfinite(numeric) else None
    return None


def _bool_feature(value: Any, include_availability: bool = False) -> list[float]:
    present = value is not None
    encoded = [1.0 if bool(value) else 0.0]
    if include_availability:
        encoded.append(1.0 if present else 0.0)
    return encoded


def _enum_name(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        for key in ("variant", "name", "value"):
            if key in value:
                return str(value[key])
    return str(value) if value is not None else ""


def _stack(rows: Iterable[np.ndarray]) -> np.ndarray:
    return _array(list(rows))


def _prediction_matrix(values: Any, expected_width: int, expected_rows: int) -> np.ndarray:
    if isinstance(values, (list, tuple)):
        # Keras multi-head predictions often come back as a list of (batch, 1) arrays.
        arrays = [_array(value) for value in values]
        if arrays and all(array.ndim >= 1 for array in arrays):
            squeezed = [array.reshape(array.shape[0], -1) if array.ndim > 1 else array.reshape(-1, 1) for array in arrays]
            if all(array.shape[1] == 1 for array in squeezed):
                matrix = np.concatenate(squeezed, axis=1)
            else:
                matrix = _array(values)
        else:
            matrix = _array(values)
    else:
        matrix = _array(values)

    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    elif matrix.ndim > 2:
        matrix = matrix.reshape(matrix.shape[0], -1)

    if matrix.shape[1] != expected_width:
        raise ValueError(f"Expected width {expected_width}, got {matrix.shape[1]}.")
    if matrix.shape[0] != expected_rows:
        raise ValueError(f"Expected batch size {expected_rows}, got {matrix.shape[0]}.")

    return matrix


def _to_plain_data(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_plain_data(item) for item in value]
    if isinstance(value, tuple):
        return [_to_plain_data(item) for item in value]
    if hasattr(value, "__dict__"):
        return {
            key: _to_plain_data(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return value


def _assert_lengths(**vectors: np.ndarray) -> None:
    for name, vector in vectors.items():
        if vector.shape[0] != INPUT_WIDTHS[name]:
            raise ValueError(
                f"{name} vector has length {vector.shape[0]}, expected {INPUT_WIDTHS[name]}."
            )


def _array(values: Any) -> np.ndarray:
    return np.asarray(values, dtype=np.float32)


def _feature_width(name: str) -> int:
    if name == "market_regime":
        return len(MARKET_REGIME_ORDER)
    if name == "dominant_news_category":
        return len(NEWS_CATEGORY_ORDER)
    if name in BOOL_WITH_AVAILABILITY_FIELDS:
        return 2
    if name in VALUE_WITH_AVAILABILITY_DEFAULTS:
        return 2
    return 1


INPUT_WIDTHS = {
    "generalist": len(_shared_context_14({}))
    + sum(_feature_width(name) for name in GENERALIST_FIELDS),
    "technical": len(_shared_context_15({}))
    + sum(_feature_width(name) for name in TECHNICAL_FIELDS),
    "earnings": len(_shared_context_14({}))
    + sum(_feature_width(name) for name in EARNINGS_FIELDS),
    "news": len(_shared_context_15({}))
    + sum(_feature_width(name) for name in NEWS_FIELDS),
    "regime": len(_shared_context_14({}))
    + sum(_feature_width(name) for name in REGIME_FIELDS),
    "aggregator_meta": len(_shared_context_14({}))
    + sum(_feature_width(name) for name in AGGREGATOR_META_FIELDS),
}
