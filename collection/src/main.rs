use collection::gather_data;

#[tokio::main]
async fn main() {
    let ticker = "TSLA";
    let data = gather_data(ticker).await;

    println!("ML Data for {}: {:#?}", ticker, data);
}
