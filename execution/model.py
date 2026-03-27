from __future__ import annotations

from tensorflow.keras import Input, Model, layers

from handle_data import INPUT_WIDTHS


def build_generalist_model() -> Model:
    inp = Input(shape=(INPUT_WIDTHS["generalist"],))

    x = layers.Dense(128, activation="relu")(inp)
    x = layers.Dense(64, activation="relu")(x)

    expected_excess_return_7d = layers.Dense(
        1,
        activation="linear",
        name="expected_excess_return_7d",
    )(x)
    prob_outperform_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_outperform_7d",
    )(x)

    model = Model(inputs=inp, outputs=[expected_excess_return_7d, prob_outperform_7d])
    model.compile(
        optimizer="adam",
        loss={
            "expected_excess_return_7d": "mse",
            "prob_outperform_7d": "binary_crossentropy",
        },
        loss_weights={
            "expected_excess_return_7d": 1.0,
            "prob_outperform_7d": 0.5,
        },
    )
    return model


def build_technical_model() -> Model:
    inp = Input(shape=(INPUT_WIDTHS["technical"],))

    x = layers.Dense(128, activation="relu")(inp)
    x = layers.Dense(64, activation="relu")(x)

    expected_excess_return_7d = layers.Dense(
        1,
        activation="linear",
        name="expected_excess_return_7d",
    )(x)
    prob_outperform_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_outperform_7d",
    )(x)

    model = Model(inputs=inp, outputs=[expected_excess_return_7d, prob_outperform_7d])
    model.compile(
        optimizer="adam",
        loss={
            "expected_excess_return_7d": "mse",
            "prob_outperform_7d": "binary_crossentropy",
        },
        loss_weights={
            "expected_excess_return_7d": 1.0,
            "prob_outperform_7d": 0.5,
        },
    )
    return model


def build_earnings_model() -> Model:
    inp = Input(shape=(INPUT_WIDTHS["earnings"],))

    x = layers.Dense(128, activation="relu")(inp)
    x = layers.Dense(64, activation="relu")(x)

    expected_excess_return_7d = layers.Dense(
        1,
        activation="linear",
        name="expected_excess_return_7d",
    )(x)
    prob_outperform_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_outperform_7d",
    )(x)

    model = Model(inputs=inp, outputs=[expected_excess_return_7d, prob_outperform_7d])
    model.compile(
        optimizer="adam",
        loss={
            "expected_excess_return_7d": "mse",
            "prob_outperform_7d": "binary_crossentropy",
        },
        loss_weights={
            "expected_excess_return_7d": 1.0,
            "prob_outperform_7d": 0.5,
        },
    )
    return model


def build_news_model() -> Model:
    inp = Input(shape=(INPUT_WIDTHS["news"],))

    x = layers.Dense(128, activation="relu")(inp)
    x = layers.Dense(64, activation="relu")(x)

    expected_excess_return_7d = layers.Dense(
        1,
        activation="linear",
        name="expected_excess_return_7d",
    )(x)
    prob_outperform_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_outperform_7d",
    )(x)
    prob_large_move_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_large_move_7d",
    )(x)

    model = Model(
        inputs=inp,
        outputs=[expected_excess_return_7d, prob_outperform_7d, prob_large_move_7d],
    )
    model.compile(
        optimizer="adam",
        loss={
            "expected_excess_return_7d": "mse",
            "prob_outperform_7d": "binary_crossentropy",
            "prob_large_move_7d": "binary_crossentropy",
        },
        loss_weights={
            "expected_excess_return_7d": 1.0,
            "prob_outperform_7d": 0.5,
            "prob_large_move_7d": 0.5,
        },
    )
    return model


def build_regime_model() -> Model:
    inp = Input(shape=(INPUT_WIDTHS["regime"],))

    x = layers.Dense(128, activation="relu")(inp)
    x = layers.Dense(64, activation="relu")(x)

    prob_signal_friendly = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_signal_friendly",
    )(x)
    prob_risk_on = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_risk_on",
    )(x)

    model = Model(inputs=inp, outputs=[prob_signal_friendly, prob_risk_on])
    model.compile(
        optimizer="adam",
        loss={
            "prob_signal_friendly": "binary_crossentropy",
            "prob_risk_on": "binary_crossentropy",
        },
        loss_weights={
            "prob_signal_friendly": 0.5,
            "prob_risk_on": 0.5,
        },
    )
    return model


def build_aggregation_model() -> Model:
    inp_meta = Input(shape=(INPUT_WIDTHS["aggregator_meta"],))
    inp_generalist = Input(shape=(2,))
    inp_technical = Input(shape=(2,))
    inp_earnings = Input(shape=(2,))
    inp_news = Input(shape=(3,))
    inp_regime = Input(shape=(2,))

    x = layers.Concatenate()(
        [
            inp_meta,
            inp_generalist,
            inp_technical,
            inp_earnings,
            inp_news,
            inp_regime,
        ]
    )
    x = layers.Dense(64, activation="relu")(x)
    x = layers.Dense(32, activation="relu")(x)

    expected_excess_return_7d = layers.Dense(
        1,
        activation="linear",
        name="expected_excess_return_7d",
    )(x)
    prob_outperform_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_outperform_7d",
    )(x)
    prob_tradeable_long_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_tradeable_long_7d",
    )(x)
    prob_tradeable_short_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_tradeable_short_7d",
    )(x)
    prob_large_move_7d = layers.Dense(
        1,
        activation="sigmoid",
        name="prob_large_move_7d",
    )(x)
    predicted_volatility_7d = layers.Dense(
        1,
        activation="softplus",
        name="predicted_volatility_7d",
    )(x)

    model = Model(
        inputs=[
            inp_meta,
            inp_generalist,
            inp_technical,
            inp_earnings,
            inp_news,
            inp_regime,
        ],
        outputs=[
            expected_excess_return_7d,
            prob_outperform_7d,
            prob_tradeable_long_7d,
            prob_tradeable_short_7d,
            prob_large_move_7d,
            predicted_volatility_7d,
        ],
    )
    model.compile(
        optimizer="adam",
        loss={
            "expected_excess_return_7d": "mse",
            "prob_outperform_7d": "binary_crossentropy",
            "prob_tradeable_long_7d": "binary_crossentropy",
            "prob_tradeable_short_7d": "binary_crossentropy",
            "prob_large_move_7d": "binary_crossentropy",
            "predicted_volatility_7d": "mse",
        },
        loss_weights={
            "expected_excess_return_7d": 1.0,
            "prob_outperform_7d": 0.5,
            "prob_tradeable_long_7d": 0.5,
            "prob_tradeable_short_7d": 0.5,
            "prob_large_move_7d": 0.5,
            "predicted_volatility_7d": 1.0,
        },
    )
    return model


def build_model_bundle() -> dict[str, Model]:
    return {
        "generalist": build_generalist_model(),
        "technical": build_technical_model(),
        "earnings": build_earnings_model(),
        "news": build_news_model(),
        "regime": build_regime_model(),
        "aggregation": build_aggregation_model(),
    }


_default_models = build_model_bundle()
generalist = _default_models["generalist"]
technical = _default_models["technical"]
earnings = _default_models["earnings"]
news = _default_models["news"]
regime = _default_models["regime"]
aggregation = _default_models["aggregation"]
