from __future__ import annotations

import argparse
from pathlib import Path

from handle_data import (
    build_aggregator_inputs,
    prepare_model_inputs,
    prepare_training_targets,
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
        "--output-dir",
        type=Path,
        default=Path("artifacts"),
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
):
    return model.fit(
        inputs,
        targets,
        epochs=epochs,
        batch_size=batch_size,
        validation_split=validation_split,
        shuffle=True,
        verbose=1,
    )


def save_models(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generalist.save(output_dir / "generalist.keras")
    technical.save(output_dir / "technical.keras")
    earnings.save(output_dir / "earnings.keras")
    news.save(output_dir / "news.keras")
    regime.save(output_dir / "regime.keras")
    aggregation.save(output_dir / "aggregation.keras")


def main() -> None:
    args = parse_args()
    inputs = prepare_model_inputs(args.dataset)
    targets = prepare_training_targets(args.dataset)

    sample_count = inputs["generalist"].shape[0]
    if sample_count == 0:
        raise ValueError("The dataset did not contain any training rows.")

    fit_model(
        generalist,
        inputs["generalist"],
        targets["generalist"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
    )
    fit_model(
        technical,
        inputs["technical"],
        targets["technical"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
    )
    fit_model(
        earnings,
        inputs["earnings"],
        targets["earnings"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
    )
    fit_model(
        news,
        inputs["news"],
        targets["news"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
    )
    fit_model(
        regime,
        inputs["regime"],
        targets["regime"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
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
    )
    fit_model(
        aggregation,
        aggregator_inputs,
        targets["aggregation"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
    )

    save_models(args.output_dir)
    print(
        f"Trained {sample_count} samples and saved models to {args.output_dir.resolve()}."
    )


if __name__ == "__main__":
    main()
