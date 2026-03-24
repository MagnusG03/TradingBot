use collection::collect_ml_data;

#[tokio::main]
async fn main() {
    let ticker = "TSLA";
    let data = collect_ml_data(ticker).await;

    println!("ML Data for {}: {:#?}", ticker, data);
}
