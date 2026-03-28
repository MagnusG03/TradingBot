use std::{
    collections::HashSet,
    env,
    error::Error,
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use collection::{collect_ml_data, collect_ml_training_data_for_ticker, normalize_ticker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        None => write_live(DEFAULT_TICKER).await,
        Some("live") => write_live(args.get(1).map(String::as_str).unwrap_or(DEFAULT_TICKER)).await,
        Some("train") => write_train(&args[1..]).await,
        Some(ticker) => write_live(ticker).await,
    }
}

const DEFAULT_TICKER: &str = "AAPL";
const DEFAULT_TARGET_TRAINING_RECORDS: usize = 5_000;

async fn write_live(ticker: &str) -> Result<(), Box<dyn Error>> {
    let ticker = normalize_ticker(ticker);
    if ticker.is_empty() {
        return Err("Ticker cannot be empty.".into());
    }

    let data = collect_ml_data(&ticker).await;
    let output = default_live_output(&ticker);
    write_json(&output, &data)?;
    println!("Saved live data for {ticker} to {}", output.display());
    Ok(())
}

async fn write_train(args: &[String]) -> Result<(), Box<dyn Error>> {
    let (tickers, target_records) = parse_train_args(args)?;
    let output = default_training_output(&tickers);
    let mut records = Vec::new();
    let total_tickers = tickers.len();

    println!(
        "Collecting training data for {total_tickers} ticker(s) with target {target_records} records..."
    );

    for (ticker_index, ticker) in tickers.into_iter().enumerate() {
        println!(
            "[train] Ticker {}/{}: {}",
            ticker_index + 1,
            total_tickers,
            ticker
        );
        let mut new_records = collect_ml_training_data_for_ticker(&ticker).await;
        let new_record_count = new_records.len();
        records.append(&mut new_records);
        println!(
            "[train] {} contributed {} records ({} total so far).",
            ticker,
            new_record_count,
            records.len()
        );
        if records.len() >= target_records {
            println!("[train] Target record count reached; stopping early.");
            break;
        }
    }

    records.sort_by(|left, right| {
        left.as_of_timestamp_utc
            .cmp(&right.as_of_timestamp_utc)
            .then_with(|| left.ticker.cmp(&right.ticker))
    });

    if records.is_empty() {
        return Err("No training data collected; refusing to overwrite the dataset file.".into());
    }

    write_json(&output, &records)?;
    println!(
        "Saved {} training records to {}.",
        records.len(),
        output.display()
    );
    if records.len() < target_records {
        println!("Collected fewer than the requested target of {target_records} records.");
    }
    Ok(())
}

fn parse_train_args(args: &[String]) -> Result<(Vec<String>, usize), Box<dyn Error>> {
    let mut tickers = Vec::new();
    let mut seen = HashSet::new();
    let mut target_records = DEFAULT_TARGET_TRAINING_RECORDS;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--target-records" => {
                target_records = required_value(args, index, "--target-records")?.parse()?;
                index += 2;
            }
            arg if arg.starts_with("--") => {
                return Err(format!("Unknown flag for train command: {arg}").into());
            }
            ticker => {
                let ticker = normalize_ticker(ticker);
                if !ticker.is_empty() && seen.insert(ticker.clone()) {
                    tickers.push(ticker);
                }
                index += 1;
            }
        }
    }

    if tickers.is_empty() {
        return Err("Provide at least one ticker for the train command.".into());
    }

    Ok((tickers, target_records))
}

fn required_value<'a>(
    args: &'a [String],
    index: usize,
    flag: &str,
) -> Result<&'a str, Box<dyn Error>> {
    args.get(index + 1)
        .map(|value| value.as_str())
        .ok_or_else(|| format!("Missing value for {flag}.").into())
}

fn write_json<T>(path: &Path, value: &T) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer_pretty(writer, value)?;
    Ok(())
}

fn default_live_output(ticker: &str) -> PathBuf {
    PathBuf::from(format!(
        "../execution/data/live/{}_live_dataset.json",
        ticker
    ))
}

fn default_training_output(tickers: &[String]) -> PathBuf {
    if tickers.len() == 1 {
        return PathBuf::from(format!(
            "../execution/data/train/{}_train_dataset.json",
            tickers[0]
        ));
    }

    PathBuf::from("../execution/data/train/training_dataset.json")
}
