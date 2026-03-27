from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tensorflow import keras

from handle_data import build_aggregator_inputs, prepare_model_inputs


ROOT = Path(__file__).resolve().parent
LIVE_DIR = ROOT / "data" / "live"
TRAIN_DIR = ROOT / "data" / "train"
MODELS_DIR = ROOT / "models"

EPOCHS = 20
BATCH_SIZE = 32
VALIDATION_SPLIT = 0.2

MODEL_FILES = (
    "generalist.keras",
    "technical.keras",
    "earnings.keras",
    "news.keras",
    "regime.keras",
    "aggregation.keras",
)


def ticker_from_live_file(path: Path) -> str:
    return path.stem.removesuffix("_live_dataset")


def model_dir(ticker: str) -> Path:
    return MODELS_DIR / ticker.upper()


def models_exist(ticker: str) -> bool:
    directory = model_dir(ticker)
    return all((directory / name).exists() for name in MODEL_FILES)


def train_if_needed(ticker: str) -> Path:
    train_file = TRAIN_DIR / f"{ticker}_train_dataset.json"
    output_dir = model_dir(ticker)

    if models_exist(ticker):
        return output_dir

    if not train_file.exists():
        raise FileNotFoundError(f"Missing train dataset: {train_file}")

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "train_model.py"),
            str(train_file),
            "--epochs",
            str(EPOCHS),
            "--batch-size",
            str(BATCH_SIZE),
            "--validation-split",
            str(VALIDATION_SPLIT),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=ROOT,
    )
    return output_dir


def load_models(ticker: str) -> dict[str, keras.Model]:
    directory = model_dir(ticker)
    return {
        "generalist": keras.models.load_model(directory / "generalist.keras"),
        "technical": keras.models.load_model(directory / "technical.keras"),
        "earnings": keras.models.load_model(directory / "earnings.keras"),
        "news": keras.models.load_model(directory / "news.keras"),
        "regime": keras.models.load_model(directory / "regime.keras"),
        "aggregation": keras.models.load_model(directory / "aggregation.keras"),
    }


for live_file in sorted(LIVE_DIR.glob("*_live_dataset.json")):
    ticker = ticker_from_live_file(live_file)
    train_if_needed(ticker)
    models = load_models(ticker)

    inputs = prepare_model_inputs(live_file)

    generalist_out = models["generalist"].predict(inputs["generalist"], verbose=0)
    technical_out = models["technical"].predict(inputs["technical"], verbose=0)
    earnings_out = models["earnings"].predict(inputs["earnings"], verbose=0)
    news_out = models["news"].predict(inputs["news"], verbose=0)
    regime_out = models["regime"].predict(inputs["regime"], verbose=0)

    aggregator_inputs = build_aggregator_inputs(
        live_file,
        generalist_out,
        technical_out,
        earnings_out,
        news_out,
        regime_out,
    )
    final_out = models["aggregation"].predict(aggregator_inputs, verbose=0)

    print(f"\n{ticker}")
    print("Generalist:", generalist_out)
    print("Technical:", technical_out)
    print("Earnings:", earnings_out)
    print("News:", news_out)
    print("Regime:", regime_out)
    print("Aggregation:", final_out)
