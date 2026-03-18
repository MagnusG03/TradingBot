use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;
use rss::Channel;

#[derive(Debug)]
struct PolymarketPrediction {
    question: String,
    outcomes: Vec<String>,
    outcome_prices: Vec<f64>,
}

#[derive(Debug)]
struct KalshiPrediction {
    title: String,
    subtitle: Option<String>,
    yes_price: f64,
    no_price: f64,
}

#[derive(Debug, Deserialize)]
struct SecSubmissions {
    name: String,
    #[serde(default)]
    tickers: Vec<String>,
    filings: SecFilings,
}

#[derive(Debug, Deserialize)]
struct SecFilings {
    recent: SecRecentFilings,
}

#[derive(Debug, Deserialize)]
struct SecRecentFilings {
    #[serde(rename = "accessionNumber", default)]
    accession_numbers: Vec<String>,

    #[serde(rename = "filingDate", default)]
    filing_dates: Vec<String>,

    #[serde(rename = "acceptanceDateTime", default)]
    acceptance_datetimes: Vec<String>,

    #[serde(rename = "form", default)]
    forms: Vec<String>,

    #[serde(rename = "items", default)]
    items: Vec<String>,

    #[serde(rename = "primaryDocument", default)]
    primary_documents: Vec<String>,

    #[serde(rename = "primaryDocDescription", default)]
    primary_doc_descriptions: Vec<String>,

    #[serde(rename = "isInlineXBRL", default)]
    is_inline_xbrl: Vec<i32>,
}

#[derive(Debug)]
struct SecFiling {
    ticker: String,
    company_name: String,
    cik: String,
    form: String,
    filing_date: String,
    acceptance_datetime: Option<String>,
    accession_number: String,
    primary_document: String,
    primary_doc_description: Option<String>,
    items: Option<String>,
    is_inline_xbrl: bool,
    filing_url: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketMarket {
    question: String,
    #[serde(default)]
    outcomes: Option<String>,
    #[serde(default, rename = "outcomePrices")]
    outcome_prices: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PolymarketEvent {
    #[serde(default)]
    markets: Vec<PolymarketMarket>,
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarket>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarket {
    title: String,
    #[serde(default)]
    subtitle: String,
    #[serde(default, rename = "yes_sub_title")]
    yes_sub_title: String,
    #[serde(default, rename = "no_sub_title")]
    no_sub_title: String,
    yes_bid: Option<i64>,
    no_bid: Option<i64>,
    #[serde(default, rename = "yes_bid_dollars")]
    yes_bid_dollars: Option<String>,
    #[serde(default, rename = "no_bid_dollars")]
    no_bid_dollars: Option<String>,
    #[serde(default, rename = "last_price_dollars")]
    last_price_dollars: Option<String>,
}

#[derive(Debug)]
struct PrNewswireRelease {
    title: String,
    source_section: String,
    link: Option<String>,
    pub_date: Option<String>,
    description: Option<String>,
}

enum FetchResult {
    Polymarket(Vec<PolymarketPrediction>),
    Kalshi(Vec<KalshiPrediction>),
    Reddit(Value),
    SecEdgar(Vec<SecFiling>),
    PrNewswire(Vec<PrNewswireRelease>),
}

// Gets all questions and outcome likelyhoods for the given Polymarket event at this current time. If a specific market is given, only returns data for that market.
async fn fetch_polymarket(
    url: &str,
    client: &Client,
) -> Result<Vec<PolymarketPrediction>, Box<dyn Error>> {
    let after_event = url
        .split("/event/")
        .nth(1)
        .unwrap_or("")
        .trim_end_matches('/');

    let parts: Vec<&str> = after_event.split('/').collect();

    let event_slug = parts.first().copied().unwrap_or("");
    let market_slug = parts.get(1).copied();

    if let Some(market_slug) = market_slug {
        let api_url = format!(
            "https://gamma-api.polymarket.com/markets/slug/{}",
            market_slug
        );

        let market = client
            .get(&api_url)
            .send()
            .await?
            .error_for_status()?
            .json::<PolymarketMarket>()
            .await?;

        let outcomes = market
            .outcomes
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_default();

        let outcome_prices = market
            .outcome_prices
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .map(|prices| {
                prices
                    .into_iter()
                    .filter_map(|p| p.parse::<f64>().ok())
                    .collect::<Vec<f64>>()
            })
            .unwrap_or_default();

        Ok(vec![PolymarketPrediction {
            question: market.question,
            outcomes,
            outcome_prices,
        }])
    } else {
        let api_url = format!(
            "https://gamma-api.polymarket.com/events/slug/{}",
            event_slug
        );

        let event = client
            .get(&api_url)
            .send()
            .await?
            .error_for_status()?
            .json::<PolymarketEvent>()
            .await?;

        let prediction_markets: Vec<PolymarketPrediction> = event
            .markets
            .into_iter()
            .map(|market| {
                let outcomes = market
                    .outcomes
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .unwrap_or_default();

                let outcome_prices = market
                    .outcome_prices
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .map(|prices| {
                        prices
                            .into_iter()
                            .filter_map(|p| p.parse::<f64>().ok())
                            .collect::<Vec<f64>>()
                    })
                    .unwrap_or_default();

                PolymarketPrediction {
                    question: market.question,
                    outcomes,
                    outcome_prices,
                }
            })
            .collect();

        Ok(prediction_markets)
    }
}

// Gets all questions and outcome likelyhoods for the given Kalshi event at this current time.
async fn fetch_kalshi(url: &str, client: &Client) -> Result<Vec<KalshiPrediction>, Box<dyn Error>> {
    fn parse_price_dollars(value: Option<&str>) -> Option<f64> {
        value.and_then(|raw| raw.parse::<f64>().ok())
    }

    fn choose_subtitle(market: &KalshiMarket) -> Option<String> {
        [
            market.subtitle.trim(),
            market.yes_sub_title.trim(),
            market.no_sub_title.trim(),
        ]
        .into_iter()
        .find(|value| !value.is_empty())
        .map(|value| value.to_string())
    }

    let event_ticker = url.rsplit('/').next().unwrap_or("").to_uppercase();

    let api_url = format!(
        "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&event_ticker={}",
        event_ticker
    );

    let response = client.get(&api_url).send().await?;

    let data: MarketsResponse = response.json().await?;

    let prediction_markets = data
        .markets
        .into_iter()
        .map(|market| {
            let subtitle = choose_subtitle(&market);

            KalshiPrediction {
                title: market.title,
                subtitle,
                yes_price: parse_price_dollars(market.yes_bid_dollars.as_deref())
                    .or_else(|| market.yes_bid.map(|value| value as f64 / 100.0))
                    .or_else(|| parse_price_dollars(market.last_price_dollars.as_deref()))
                    .unwrap_or(0.0),
                no_price: parse_price_dollars(market.no_bid_dollars.as_deref())
                    .or_else(|| market.no_bid.map(|value| value as f64 / 100.0))
                    .or_else(|| {
                        parse_price_dollars(market.last_price_dollars.as_deref())
                            .map(|last_price| 1.0 - last_price)
                    })
                    .unwrap_or(0.0),
            }
        })
        .collect();

    Ok(prediction_markets)
}

// Currently unimplemented due to Reddit API restrictions.
async fn fetch_reddit(
    url: &str,
    client: &Client,
    access_token: &str,
) -> Result<Value, Box<dyn Error>> {
    let post_id = url
        .split("/comments/")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .ok_or("Could not extract Reddit post ID from URL")?;

    let api_url = format!("https://oauth.reddit.com/api/info?id=t3_{}", post_id);

    let json: Value = client
        .get(&api_url)
        .bearer_auth(access_token)
        .header("User-Agent", "MagnusTradingBot")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(json)
}

async fn get_reddit_access_token(
    client: &Client,
    client_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn Error>> {
    let response = client
        .post("https://ssl.reddit.com/api/v1/access_token")
        .basic_auth(client_id, Some(client_secret))
        .header("User-Agent", "MagnusTradingBot")
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    Ok(response.to_string())
}

// Gets the 100? most recent relevant filings for the given ticker.
async fn fetch_sec_edgar(ticker: &str, client: &Client) -> Result<Vec<SecFiling>, Box<dyn Error>> {
    let cik = lookup_sec_cik(ticker, client).await?;
    let url = format!("https://data.sec.gov/submissions/CIK{}.json", cik);

    let submissions = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json::<SecSubmissions>()
        .await?;

    let company_name = submissions.name;
    let recent = submissions.filings.recent;
    let canonical_ticker = submissions
        .tickers
        .first()
        .cloned()
        .unwrap_or_else(|| ticker.to_ascii_uppercase());

    let mut filings = Vec::new();

    for i in 0..recent.forms.len() {
        let form = recent.forms.get(i).cloned().unwrap_or_default();

        if !is_relevant_form(&form) {
            continue;
        }

        let accession_number = recent.accession_numbers.get(i).cloned().unwrap_or_default();

        let primary_document = recent.primary_documents.get(i).cloned().unwrap_or_default();

        if accession_number.is_empty() || primary_document.is_empty() {
            continue;
        }

        let accession_no_dashes = accession_number.replace('-', "");
        let filing_url = format!(
            "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
            cik.trim_start_matches('0'),
            accession_no_dashes,
            primary_document
        );

        let items = recent
            .items
            .get(i)
            .cloned()
            .filter(|s| !s.trim().is_empty());

        let primary_doc_description = recent
            .primary_doc_descriptions
            .get(i)
            .cloned()
            .filter(|s| !s.trim().is_empty());

        let acceptance_datetime = recent
            .acceptance_datetimes
            .get(i)
            .cloned()
            .filter(|s| !s.trim().is_empty());

        let is_inline_xbrl = recent.is_inline_xbrl.get(i).copied().unwrap_or(0) != 0;

        filings.push(SecFiling {
            ticker: canonical_ticker.clone(),
            company_name: company_name.clone(),
            cik: cik.clone(),
            form,
            filing_date: recent.filing_dates.get(i).cloned().unwrap_or_default(),
            acceptance_datetime,
            accession_number,
            primary_document,
            primary_doc_description,
            items,
            is_inline_xbrl,
            filing_url,
        });
    }

    Ok(filings)
}

async fn lookup_sec_cik(
    ticker: &str,
    client: &Client,
) -> Result<String, Box<dyn std::error::Error>> {
    let ticker_map: Value = client
        .get("https://www.sec.gov/files/company_tickers.json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let wanted = ticker.trim().to_ascii_uppercase();

    let cik = ticker_map
        .as_object()
        .ok_or("Invalid SEC ticker map")?
        .values()
        .find_map(|entry| {
            let t = entry.get("ticker")?.as_str()?;
            if t.eq_ignore_ascii_case(&wanted) {
                entry.get("cik_str")?.as_u64()
            } else {
                None
            }
        })
        .ok_or_else(|| format!("Ticker not found: {}", ticker))?;

    Ok(format!("{:010}", cik))
}

fn is_relevant_form(form: &str) -> bool {
    return matches!(
        form,
        "8-K"
            | "8-K/A"
            | "6-K"
            | "6-K/A"
            | "10-Q"
            | "10-Q/A"
            | "10-K"
            | "10-K/A"
            | "S-1"
            | "S-1/A"
            | "3"
            | "4"
            | "5"
            | "SC 13D"
            | "SC 13D/A"
            | "SC 13G"
            | "SC 13G/A"
    );
}

// Fetches all recent PR Newswire releases from an RSS feed URL. (20 most recent?)
async fn fetch_prnewswire(
    client: &Client,
    feed_url: &str,
) -> Result<Vec<PrNewswireRelease>, Box<dyn Error>> {
    let bytes = client
        .get(feed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let channel = Channel::read_from(&bytes[..])?;
    let source_section = channel.title().to_string();

    let releases = channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = item.title()?.trim();
            if title.is_empty() {
                return None;
            }

            Some(PrNewswireRelease {
                title: title.to_string(),
                source_section: source_section.clone(),
                link: item.link().map(|s| s.to_string()),
                pub_date: item.pub_date().map(|s| s.to_string()),
                description: item.description().map(|s| s.to_string()),
            })
        })
        .collect();

    Ok(releases)
}

async fn fetch(identifier: &str) -> Result<FetchResult, Box<dyn Error>> {
    let client = Client::builder().user_agent("MagnusTradingBot").build()?;

    match () {
        _ if identifier.contains("reddit.com") => {
            let reddit_access_token =
                get_reddit_access_token(&client, "your_client_id", "your_client_secret").await?;
            Ok(FetchResult::Reddit(
                fetch_reddit(identifier, &client, &reddit_access_token).await?,
            ))
        }
        _ if identifier.contains("polymarket.com") => Ok(FetchResult::Polymarket(
            fetch_polymarket(identifier, &client).await?,
        )),
        _ if identifier.contains("kalshi.com") => Ok(FetchResult::Kalshi(
            fetch_kalshi(identifier, &client).await?,
        )),
        _ if identifier.len() > 0 && identifier.len() <= 5 => Ok(FetchResult::SecEdgar(
            fetch_sec_edgar(identifier, &client).await?,
        )),
        _ if identifier.contains("prnewswire.com") => Ok(FetchResult::PrNewswire(
            fetch_prnewswire(&client, identifier).await?,
        )),
        _ => Err(std::io::Error::other("Unsupported identifier").into()),
    }
}

#[tokio::main]
async fn main() {
    let identifier = "https://www.prnewswire.com/rss/news-releases-list.rss";

    match fetch(identifier).await {
        Ok(FetchResult::Polymarket(markets)) => {
            if markets.is_empty() {
                println!("No markets found.");
            } else {
                for market in markets {
                    println!("Question: {}", market.question);
                    println!("Outcomes: {:?}", market.outcomes);
                    println!("Outcome prices: {:?}", market.outcome_prices);
                }
            }
        }
        Ok(FetchResult::Kalshi(markets)) => {
            if markets.is_empty() {
                println!("No markets found.");
            } else {
                for market in markets {
                    println!("Question: {}", market.title);
                    if let Some(subtitle) = &market.subtitle {
                        println!("Subtitle: {}", subtitle);
                    }
                    println!("Outcomes: {:?}", ["Yes", "No"]);
                    println!("Outcome prices: {:?}", [market.yes_price, market.no_price]);
                }
            }
        }
        Ok(FetchResult::Reddit(json)) => {
            println!("Reddit post data: {}", json);
        }
        Ok(FetchResult::SecEdgar(filings)) => {
            if filings.is_empty() {
                println!("No relevant SEC filings found.");
            } else {
                for filing in filings {
                    println!("Ticker: {}", filing.ticker);
                    println!("Company: {}", filing.company_name);
                    println!("CIK: {}", filing.cik);
                    println!("Form: {}", filing.form);
                    println!("Filing date: {}", filing.filing_date);
                    println!("Acceptance datetime: {:?}", filing.acceptance_datetime);
                    println!("Accession number: {}", filing.accession_number);
                    println!("Primary document: {}", filing.primary_document);
                    println!(
                        "Primary doc description: {:?}",
                        filing.primary_doc_description
                    );
                    println!("Items: {:?}", filing.items);
                    println!("Inline XBRL: {}", filing.is_inline_xbrl);
                    println!("Filing URL: {}", filing.filing_url);
                    println!();
                }
            }
        }
        Ok(FetchResult::PrNewswire(releases)) => {
            if releases.is_empty() {
                println!("No PR Newswire releases found.");
            } else {
                for release in releases {
                    println!("Title: {}", release.title);
                    println!("Section: {}", release.source_section);
                    println!("Published: {:?}", release.pub_date);
                    println!("Link: {:?}", release.link);
                    println!("Description: {:?}", release.description);
                    println!();
                }
            }
        }
        Err(e) => eprintln!("Error fetching market: {}", e),
    }
}
