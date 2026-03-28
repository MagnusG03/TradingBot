from __future__ import annotations

import argparse
from pathlib import Path

from tensorflow.keras.callbacks import EarlyStopping

from handle_data import (
    apply_aggregator_input_normalization,
    apply_model_input_normalization,
    build_aggregator_inputs,
    fit_aggregator_input_normalization,
    fit_model_input_normalization,
    NORMALIZATION_FILENAME,
    prepare_model_inputs,
    prepare_training_targets,
    save_normalization_bundle,
    training_row_count,
)
from model import aggregation, earnings, generalist, news, regime, technical


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train the specialist and aggregation models from collected ML records.",
    )
    parser.add_argument("dataset", type=Path, help="Path to the JSON dataset file.")
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--validation-split", type=float, default=0.2)
    parser.add_argument(
        "--early-stopping-patience",
        type=int,
        default=3,
        help="Stop training after this many epochs without validation loss improvement.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("models"),
        help="Directory where trained .keras models will be written.",
    )
    return parser.parse_args()


def fit_model(
    model,
    inputs,
    targets,
    *,
    epochs: int,
    batch_size: int,
    validation_split: float,
    early_stopping_patience: int,
):
    callbacks = [
        EarlyStopping(
            monitor="val_loss",
            patience=early_stopping_patience,
            restore_best_weights=True,
        )
    ]
    return model.fit(
        inputs,
        targets,
        epochs=epochs,
        batch_size=batch_size,
        validation_split=validation_split,
        shuffle=True,
        verbose=1,
        callbacks=callbacks,
    )


def save_models(
    output_dir: Path,
    normalization_bundle: dict[str, dict[str, dict[str, list[float]]]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generalist.save(output_dir / "generalist.keras")
    technical.save(output_dir / "technical.keras")
    earnings.save(output_dir / "earnings.keras")
    news.save(output_dir / "news.keras")
    regime.save(output_dir / "regime.keras")
    aggregation.save(output_dir / "aggregation.keras")
    save_normalization_bundle(output_dir / NORMALIZATION_FILENAME, normalization_bundle)


def main() -> None:
    args = parse_args()
    raw_inputs = prepare_model_inputs(args.dataset)
    targets = prepare_training_targets(args.dataset)

    sample_count = raw_inputs["generalist"].shape[0]
    if sample_count == 0:
        raise ValueError("The dataset did not contain any training rows.")

    train_rows = training_row_count(sample_count, args.validation_split)
    specialist_normalization = fit_model_input_normalization(
        raw_inputs,
        train_rows=train_rows,
    )
    inputs = apply_model_input_normalization(raw_inputs, specialist_normalization)

    fit_model(
        generalist,
        inputs["generalist"],
        targets["generalist"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        technical,
        inputs["technical"],
        targets["technical"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        earnings,
        inputs["earnings"],
        targets["earnings"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        news,
        inputs["news"],
        targets["news"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        regime,
        inputs["regime"],
        targets["regime"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )

    generalist_pred = generalist.predict(inputs["generalist"], verbose=0)
    technical_pred = technical.predict(inputs["technical"], verbose=0)
    earnings_pred = earnings.predict(inputs["earnings"], verbose=0)
    news_pred = news.predict(inputs["news"], verbose=0)
    regime_pred = regime.predict(inputs["regime"], verbose=0)

    aggregator_inputs = build_aggregator_inputs(
        args.dataset,
        generalist_pred,
        technical_pred,
        earnings_pred,
        news_pred,
        regime_pred,
        prepared_inputs=raw_inputs,
    )
    aggregation_normalization = fit_aggregator_input_normalization(
        aggregator_inputs,
        train_rows=train_rows,
    )
    aggregator_inputs = apply_aggregator_input_normalization(
        aggregator_inputs,
        aggregation_normalization,
    )
    fit_model(
        aggregation,
        aggregator_inputs,
        targets["aggregation"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )

    save_models(
        args.output_dir,
        {
            "specialist": specialist_normalization,
            "aggregation": aggregation_normalization,
        },
    )
    print(
        f"Trained {sample_count} samples and saved models to {args.output_dir.resolve()}."
    )


if __name__ == "__main__":
    main()
