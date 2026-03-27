use collection::{collect_ml_training_data, collect_ml_data};
use std::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ticker = "AAPL";
    //let data = collect_ml_training_data(&[ticker]).await;
    //if data.is_empty() {
    //    return Err("No training data collected; refusing to overwrite the dataset file.".into());
    //}
    //let file = File::create("../execution/data/train/AAPL_train_dataset.json")?;
    //serde_json::to_writer_pretty(file, &data)?;

    //Ok(())

    let data = collect_ml_data(ticker).await;
    let file = File::create("../execution/data/live/AAPL_live_dataset.json")?;
    serde_json::to_writer_pretty(file, &data)?;

    Ok(())
}
