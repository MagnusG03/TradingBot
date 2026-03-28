from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
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
from model import (
    build_aggregation_model,
    build_earnings_model,
    build_generalist_model,
    build_news_model,
    build_regime_model,
    build_technical_model,
)


SPECIALIST_MODEL_NAMES = (
    "generalist",
    "technical",
    "earnings",
    "news",
    "regime",
)

SPECIALIST_OUTPUT_WIDTHS = {
    "generalist": 2,
    "technical": 2,
    "earnings": 2,
    "news": 3,
    "regime": 2,
}


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
        "--oof-folds",
        type=int,
        default=5,
        help="Number of expanding-window folds used to build out-of-fold specialist predictions.",
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
    verbose: int = 1,
):
    callbacks = []
    if early_stopping_patience > 0:
        callbacks.append(
            EarlyStopping(
                monitor="val_loss" if validation_split > 0.0 else "loss",
                patience=early_stopping_patience,
                restore_best_weights=True,
            )
        )

    return model.fit(
        inputs,
        targets,
        epochs=epochs,
        batch_size=batch_size,
        validation_split=validation_split,
        shuffle=True,
        verbose=verbose,
        callbacks=callbacks,
    )


def save_models(
    output_dir: Path,
    models: dict[str, object],
    normalization_bundle: dict[str, dict[str, dict[str, list[float]]]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    models["generalist"].save(output_dir / "generalist.keras")
    models["technical"].save(output_dir / "technical.keras")
    models["earnings"].save(output_dir / "earnings.keras")
    models["news"].save(output_dir / "news.keras")
    models["regime"].save(output_dir / "regime.keras")
    models["aggregation"].save(output_dir / "aggregation.keras")
    save_normalization_bundle(output_dir / NORMALIZATION_FILENAME, normalization_bundle)


def build_training_models() -> dict[str, object]:
    return {
        "generalist": build_generalist_model(),
        "technical": build_technical_model(),
        "earnings": build_earnings_model(),
        "news": build_news_model(),
        "regime": build_regime_model(),
        "aggregation": build_aggregation_model(),
    }


def build_specialist_models() -> dict[str, object]:
    return {
        "generalist": build_generalist_model(),
        "technical": build_technical_model(),
        "earnings": build_earnings_model(),
        "news": build_news_model(),
        "regime": build_regime_model(),
    }


def _slice_inputs(
    inputs: dict[str, np.ndarray],
    rows: slice | np.ndarray,
) -> dict[str, np.ndarray]:
    return {name: values[rows] for name, values in inputs.items()}


def _slice_targets(
    targets: dict[str, np.ndarray],
    rows: slice | np.ndarray,
) -> dict[str, np.ndarray]:
    return {name: values[rows] for name, values in targets.items()}


def _prediction_matrix(
    values: object,
    *,
    expected_width: int,
) -> np.ndarray:
    if isinstance(values, (list, tuple)):
        arrays = [np.asarray(value, dtype=np.float32) for value in values]
        reshaped = [
            array.reshape(array.shape[0], -1) if array.ndim > 1 else array.reshape(-1, 1)
            for array in arrays
        ]
        if reshaped and all(array.shape[1] == 1 for array in reshaped):
            matrix = np.concatenate(reshaped, axis=1)
        else:
            matrix = np.asarray(values, dtype=np.float32)
    else:
        matrix = np.asarray(values, dtype=np.float32)

    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    elif matrix.ndim > 2:
        matrix = matrix.reshape(matrix.shape[0], -1)

    if matrix.shape[1] != expected_width:
        raise ValueError(f"Expected width {expected_width}, got {matrix.shape[1]}.")

    return matrix.astype(np.float32, copy=False)


def _oof_ranges(sample_count: int, fold_count: int) -> list[tuple[slice, slice]]:
    if fold_count < 1:
        raise ValueError("oof_folds must be at least 1.")

    test_size = sample_count // (fold_count + 1)
    if test_size < 1:
        raise ValueError(
            f"Not enough samples ({sample_count}) to build {fold_count} out-of-fold splits."
        )

    remainder = sample_count % (fold_count + 1)
    ranges: list[tuple[slice, slice]] = []
    for fold_index in range(fold_count):
        train_end = remainder + test_size * (fold_index + 1)
        predict_end = remainder + test_size * (fold_index + 2)
        ranges.append((slice(0, train_end), slice(train_end, predict_end)))
    return ranges


def build_specialist_oof_predictions(
    raw_inputs: dict[str, np.ndarray],
    targets: dict[str, dict[str, np.ndarray]],
    *,
    epochs: int,
    batch_size: int,
    validation_split: float,
    early_stopping_patience: int,
    oof_folds: int,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    sample_count = raw_inputs["generalist"].shape[0]
    predictions = {
        name: np.zeros((sample_count, width), dtype=np.float32)
        for name, width in SPECIALIST_OUTPUT_WIDTHS.items()
    }
    oof_mask = np.zeros(sample_count, dtype=bool)

    for train_rows, predict_rows in _oof_ranges(sample_count, oof_folds):
        fold_train_inputs = _slice_inputs(raw_inputs, train_rows)
        fold_normalization = fit_model_input_normalization(fold_train_inputs)
        fold_inputs = apply_model_input_normalization(raw_inputs, fold_normalization)
        fold_models = build_specialist_models()

        fold_train_count = fold_train_inputs["generalist"].shape[0]
        fold_validation_split = (
            validation_split
            if fold_train_count > 1 and int(fold_train_count * validation_split) >= 1
            else 0.0
        )

        for model_name in SPECIALIST_MODEL_NAMES:
            fit_model(
                fold_models[model_name],
                fold_inputs[model_name][train_rows],
                _slice_targets(targets[model_name], train_rows),
                epochs=epochs,
                batch_size=batch_size,
                validation_split=fold_validation_split,
                early_stopping_patience=early_stopping_patience,
                verbose=0,
            )
            fold_pred = fold_models[model_name].predict(
                fold_inputs[model_name][predict_rows],
                verbose=0,
            )
            predictions[model_name][predict_rows] = _prediction_matrix(
                fold_pred,
                expected_width=SPECIALIST_OUTPUT_WIDTHS[model_name],
            )

        oof_mask[predict_rows] = True

    return predictions, oof_mask


def main() -> None:
    args = parse_args()
    raw_inputs = prepare_model_inputs(args.dataset)
    targets = prepare_training_targets(args.dataset)
    models = build_training_models()

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
        models["generalist"],
        inputs["generalist"],
        targets["generalist"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        models["technical"],
        inputs["technical"],
        targets["technical"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        models["earnings"],
        inputs["earnings"],
        targets["earnings"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        models["news"],
        inputs["news"],
        targets["news"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )
    fit_model(
        models["regime"],
        inputs["regime"],
        targets["regime"],
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )

    specialist_oof_predictions, oof_mask = build_specialist_oof_predictions(
        raw_inputs,
        targets,
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
        oof_folds=args.oof_folds,
    )

    aggregator_inputs = build_aggregator_inputs(
        args.dataset,
        specialist_oof_predictions["generalist"],
        specialist_oof_predictions["technical"],
        specialist_oof_predictions["earnings"],
        specialist_oof_predictions["news"],
        specialist_oof_predictions["regime"],
        prepared_inputs=raw_inputs,
    )
    aggregator_inputs = [values[oof_mask] for values in aggregator_inputs]
    aggregator_targets = _slice_targets(targets["aggregation"], oof_mask)
    aggregator_sample_count = aggregator_inputs[0].shape[0]
    if aggregator_sample_count == 0:
        raise ValueError("Out-of-fold specialist predictions did not produce any rows.")

    aggregator_train_rows = training_row_count(
        aggregator_sample_count,
        args.validation_split,
    )
    aggregation_normalization = fit_aggregator_input_normalization(
        aggregator_inputs,
        train_rows=aggregator_train_rows,
    )
    aggregator_inputs = apply_aggregator_input_normalization(
        aggregator_inputs,
        aggregation_normalization,
    )
    fit_model(
        models["aggregation"],
        aggregator_inputs,
        aggregator_targets,
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.validation_split,
        early_stopping_patience=args.early_stopping_patience,
    )

    save_models(
        args.output_dir,
        models,
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
