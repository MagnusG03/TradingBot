use atom_syndication::Feed;
use chrono::Utc;
use reqwest::{
    Client,
    header::{
        ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, CONNECTION, HeaderMap, HeaderValue,
        UPGRADE_INSECURE_REQUESTS, USER_AGENT,
    },
};
use rss::{Channel, Item};
use scraper::Html;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

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

#[derive(Debug)]
struct GlobeNewswireRelease {
    feed_name: String,
    title: String,
    link: String,
    pub_date: Option<String>,
    description: Option<String>,
    categories: Vec<String>,
}

#[derive(Debug)]
struct NasdaqTradeHalt {
    ticker: String,
    company_name: String,
    market: String,
    halt_date: String,
    halt_time: String,
    reason: String,
    resumption_date: Option<String>,
    resumption_quote_time: Option<String>,
    resumption_trade_time: Option<String>,
    pause_threshold_price: Option<String>,
}

#[derive(Debug)]
struct GoogleArticle {
    title: String,
    link: String,
    pub_date: Option<String>,
    description: Option<String>,
}

enum FetchResult {
    Polymarket(Vec<PolymarketPrediction>),
    Kalshi(Vec<KalshiPrediction>),
    Reddit(Value),
    SecEdgar(Vec<SecFiling>),
    PrNewswire(Vec<PrNewswireRelease>),
    GlobeNewswire(Vec<GlobeNewswireRelease>),
    NasdaqTradeHalt(Vec<NasdaqTradeHalt>),
    GoogleNews(Vec<GoogleArticle>),
}

fn build_client() -> Result<Client, Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();

    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        ),
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        ),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(UPGRADE_INSECURE_REQUESTS, HeaderValue::from_static("1"));

    let client = Client::builder()
        .default_headers(headers)
        .cookie_store(true)
        .brotli(true)
        .gzip(true)
        .deflate(true)
        .build()?;

    Ok(client)
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

// Fetches the most recent relevant SEC filings (returns like 24? most recent filings).
async fn fetch_sec_edgar_all(
    identifier: &str,
    client: &Client,
) -> Result<Vec<SecFiling>, Box<dyn Error>> {
    let feed_url = if identifier.contains("sec.gov") {
        identifier.to_string()
    } else {
        format!(
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&owner=exclude&count=100&output=atom",
            identifier.trim()
        )
    };

    let ticker_map: Value = client
        .get("https://www.sec.gov/files/company_tickers.json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut cik_to_ticker = std::collections::HashMap::new();

    if let Some(entries) = ticker_map.as_object() {
        for entry in entries.values() {
            let Some(cik_num) = entry.get("cik_str").and_then(|v| v.as_u64()) else {
                continue;
            };
            let Some(ticker) = entry.get("ticker").and_then(|v| v.as_str()) else {
                continue;
            };

            cik_to_ticker.insert(format!("{:010}", cik_num), ticker.to_string());
        }
    }

    let bytes = client
        .get(&feed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let feed = Feed::read_from(&bytes[..])?;
    let mut filings = Vec::new();

    for entry in feed.entries() {
        let title = entry.title().to_string();

        let Some((form, rest)) = title.split_once(" - ") else {
            continue;
        };

        if !is_relevant_form(form.trim()) {
            continue;
        }

        let Some(company_start) = rest.find(" (") else {
            continue;
        };

        let company_name = rest[..company_start].trim().to_string();
        let after_company = &rest[company_start + 2..];

        let Some(cik_end) = after_company.find(')') else {
            continue;
        };

        let cik = after_company[..cik_end].trim().to_string();

        let filing_url = entry
            .links()
            .first()
            .map(|link| link.href().to_string())
            .unwrap_or_default();

        let accession_number = filing_url
            .split('/')
            .find(|part| part.len() == 18 && part.chars().all(|c| c.is_ascii_digit()))
            .map(|s| format!("{}-{}-{}", &s[0..10], &s[10..12], &s[12..18]))
            .unwrap_or_default();

        let primary_document = filing_url.rsplit('/').next().unwrap_or("").to_string();

        let filing_date = entry.updated().date_naive().to_string();
        let acceptance_datetime = Some(entry.updated().to_rfc3339());

        let primary_doc_description = entry
            .summary()
            .map(|s| s.value.to_string())
            .filter(|s| !s.trim().is_empty());

        filings.push(SecFiling {
            ticker: cik_to_ticker.get(&cik).cloned().unwrap_or_default(),
            company_name,
            cik,
            form: form.trim().to_string(),
            filing_date,
            acceptance_datetime,
            accession_number,
            primary_document,
            primary_doc_description,
            items: None,
            is_inline_xbrl: false,
            filing_url,
        });
    }

    Ok(filings)
}

// Gets the 100? most recent relevant filings for the given ticker.
async fn fetch_sec_edgar_ticker(
    ticker: &str,
    client: &Client,
) -> Result<Vec<SecFiling>, Box<dyn Error>> {
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

// Fetches 20 most recent GlobeNewswire releases from RSS.
async fn fetch_globenewswire(
    client: &Client,
    feed_url: &str,
) -> Result<Vec<GlobeNewswireRelease>, Box<dyn Error>> {
    let bytes = client
        .get(feed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let channel = Channel::read_from(&bytes[..])?;
    let feed_name = channel.title().trim().to_string();

    let releases = channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = item.title()?.trim();
            if title.is_empty() {
                return None;
            }

            Some(GlobeNewswireRelease {
                feed_name: feed_name.clone(),
                title: title.to_string(),
                link: item.link().unwrap_or_default().trim().to_string(),
                pub_date: item.pub_date().map(|s| s.to_string()),
                description: item.description().map(|s| s.to_string()),
                categories: item
                    .categories()
                    .iter()
                    .map(|category| category.name().trim().to_string())
                    .filter(|category| !category.is_empty())
                    .collect(),
            })
        })
        .collect();

    Ok(releases)
}

// Fetches todays most recent NASDAQ trade halts from the NASDAQ RSS feed URL.
async fn fetch_nasdaq_trade_halt(
    client: &Client,
    feed_url: &str,
) -> Result<Vec<NasdaqTradeHalt>, Box<dyn Error>> {
    let bytes = client
        .get(feed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let channel = Channel::read_from(&bytes[..])?;

    let today = Utc::now().format("%m/%d/%Y").to_string();

    let halts = channel
        .items()
        .iter()
        .filter_map(|item| {
            let halt_date = get_ndaq_field(item, "HaltDate")?;

            if halt_date != today {
                return None;
            }

            Some(NasdaqTradeHalt {
                ticker: get_ndaq_field(item, "IssueSymbol")?,
                company_name: get_ndaq_field(item, "IssueName")?,
                market: get_ndaq_field(item, "Market")?,
                halt_date,
                halt_time: get_ndaq_field(item, "HaltTime")?,
                reason: get_ndaq_field(item, "ReasonCode")?,
                resumption_date: get_ndaq_field(item, "ResumptionDate"),
                resumption_quote_time: get_ndaq_field(item, "ResumptionQuoteTime"),
                resumption_trade_time: get_ndaq_field(item, "ResumptionTradeTime"),
                pause_threshold_price: get_ndaq_field(item, "PauseThresholdPrice"),
            })
        })
        .collect();

    Ok(halts)
}

fn get_ndaq_field(item: &Item, field: &str) -> Option<String> {
    item.extensions()
        .get("ndaq")
        .and_then(|fields| fields.get(field))
        .and_then(|values| values.first())
        .and_then(|ext| ext.value.clone())
}

// Fetches all Google News articles from the given RSS feed URL. (max 100 most recent)
async fn fetch_google_news(
    url: &str,
    client: &Client,
) -> Result<Vec<GoogleArticle>, Box<dyn Error>> {
    let bytes = client
        .get(url)
        .header("Referer", "https://news.google.com/")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let channel = Channel::read_from(&bytes[..])?;
    let articles: Vec<GoogleArticle> = channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = item.title()?.trim();
            if title.is_empty() {
                return None;
            }

            Some(GoogleArticle {
                title: title.to_string(),
                link: item.link().unwrap_or(url).trim().to_string(),
                pub_date: item.pub_date().map(|value| value.trim().to_string()),
                description: item
                    .description()
                    .map(|value| {
                        Html::parse_fragment(value)
                            .root_element()
                            .text()
                            .collect::<String>()
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .filter(|value| !value.is_empty()),
            })
        })
        .collect();

    if articles.is_empty() {
        return Err(std::io::Error::other("No Google News stories found").into());
    }

    Ok(articles)
}

async fn fetch(identifier: &str) -> Result<FetchResult, Box<dyn Error>> {
    let client = build_client()?;

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
        _ if identifier.contains("sec.gov") || (identifier.len() > 0 && identifier.len() <= 5) => {
            Ok(FetchResult::SecEdgar(
                fetch_sec_edgar_all(identifier, &client).await?,
            ))
        }
        _ if identifier.contains("prnewswire.com") => Ok(FetchResult::PrNewswire(
            fetch_prnewswire(&client, identifier).await?,
        )),
        _ if identifier.contains("nasdaqtrader.com") => Ok(FetchResult::NasdaqTradeHalt(
            fetch_nasdaq_trade_halt(&client, identifier).await?,
        )),
        _ if identifier.contains("globenewswire.com") => Ok(FetchResult::GlobeNewswire(
            fetch_globenewswire(&client, identifier).await?,
        )),
        _ if identifier.contains("google.com") => Ok(FetchResult::GoogleNews(
            fetch_google_news(identifier, &client).await?,
        )),
        _ => Err(std::io::Error::other("Unsupported identifier").into()),
    }
}

async fn sentiment_analysis(string: &str) -> f64 {
    fn tokenize(input: &str) -> Vec<String> {
        input
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(|token| token.to_ascii_lowercase())
            .collect()
    }

    fn word_score(token: &str) -> Option<f64> {
        Some(match token {
            "acquire" | "acquires" | "acquired" | "buy" | "buys" | "bought" | "bullish"
            | "collaboration" | "growth" | "improve" | "improves" | "improved" | "innovation"
            | "innovative" | "opportunity" | "outperform" | "outperformed" | "praise"
            | "praises" | "praised" | "rebound" | "revenue" | "strong" | "surge" | "surges"
            | "surged" | "win" | "wins" | "won" => 1.0,
            "best" | "beat" | "beats" | "boost" | "boosts" | "jump" | "jumps" | "jumped"
            | "lift" | "lifts" | "lifted" | "largest" | "gain" | "gains" | "gained" | "up" => 0.7,
            "concern" | "concerns" | "cut" | "cuts" | "cutting" | "decrease" | "decreases"
            | "decreased" | "downgrade" | "downgrades" | "downgraded" | "drop" | "drops"
            | "dropped" | "exposure" | "fall" | "falls" | "fell" | "leak" | "leaks" | "miss"
            | "misses" | "missed" | "pressure" | "regulatory" | "risk" | "risks" | "sell"
            | "sells" | "sold" | "weak" => -1.0,
            "lawsuit" | "lawsuits" | "probe" | "probes" | "scandal" | "slump" | "slumps"
            | "warning" | "warnings" => -1.4,
            _ => return None,
        })
    }

    fn phrase_score(tokens: &[String], index: usize) -> Option<(usize, f64)> {
        const PHRASES: &[(&[&str], f64)] = &[
            (&["beat", "expectations"], 1.6),
            (&["beats", "expectations"], 1.6),
            (&["cut", "exposure"], -1.6),
            (&["fee", "cut"], -0.5),
            (&["legal", "win"], 1.7),
            (&["miss", "expectations"], -1.6),
            (&["misses", "expectations"], -1.6),
            (&["price", "target", "raised"], 1.5),
            (&["price", "target", "cut"], -1.5),
            (&["raised", "guidance"], 1.7),
            (&["regulatory", "pressure"], -1.7),
            (&["sales", "surge"], 1.8),
            (&["shares", "bought"], 0.8),
            (&["shares", "sold"], -0.8),
            (&["stock", "jumps"], 1.8),
            (&["stock", "position", "raised"], 0.9),
            (&["stock", "holdings", "lifted"], 0.9),
        ];

        PHRASES
            .iter()
            .filter(|(phrase, _)| index + phrase.len() <= tokens.len())
            .filter(|(phrase, _)| {
                phrase
                    .iter()
                    .enumerate()
                    .all(|(offset, token)| tokens[index + offset] == *token)
            })
            .max_by_key(|(phrase, _)| phrase.len())
            .map(|(phrase, score)| (phrase.len(), *score))
    }

    fn is_negation(token: &str) -> bool {
        matches!(
            token,
            "no" | "not" | "never" | "none" | "without" | "hardly"
        )
    }

    fn intensity(token: &str) -> f64 {
        match token {
            "deeply" | "extremely" | "highly" | "sharply" | "strongly" => 1.35,
            "very" => 1.2,
            "slightly" => 0.75,
            _ => 1.0,
        }
    }

    let tokens = tokenize(string);

    if tokens.is_empty() {
        return 0.0;
    }

    let mut total_score = 0.0;
    let mut matched_terms = 0.0;
    let mut index = 0;

    while index < tokens.len() {
        if let Some((phrase_len, score)) = phrase_score(&tokens, index) {
            total_score += score;
            matched_terms += 1.0;
            index += phrase_len;
            continue;
        }

        if let Some(mut score) = word_score(&tokens[index]) {
            if index > 0 {
                score *= intensity(&tokens[index - 1]);
            }

            let negated = tokens[index.saturating_sub(3)..index]
                .iter()
                .any(|token| is_negation(token));
            if negated {
                score *= -0.8;
            }

            total_score += score;
            matched_terms += 1.0;
        }

        index += 1;
    }

    if matched_terms == 0.0 {
        return 0.0;
    }

    (total_score / matched_terms).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::sentiment_analysis;

    #[tokio::test]
    async fn positive_financial_headline_scores_above_zero() {
        let score = sentiment_analysis(
            "Apple CEO visits China as iPhone sales surge and the company emphasizes innovation",
        )
        .await;

        assert!(score > 0.4, "expected positive score, got {score}");
    }

    #[tokio::test]
    async fn negative_financial_headline_scores_below_zero() {
        let score = sentiment_analysis(
            "Analyst downgrades AAPL after leak spotlights wearable risk and regulatory pressure",
        )
        .await;

        assert!(score < -0.4, "expected negative score, got {score}");
    }

    #[tokio::test]
    async fn unknown_text_stays_neutral() {
        let score = sentiment_analysis("Apple discussed several topics during a meeting").await;

        assert_eq!(score, 0.0);
    }
}

async fn dispatcher() {}

async fn storer(_fetch_result: FetchResult) {}

#[tokio::main]
async fn main() {
    let identifier = "https://news.google.com/rss/search?q=AAPL+when:1d&hl=en-US&gl=US&ceid=US:en";

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
        Ok(FetchResult::NasdaqTradeHalt(halts)) => {
            if halts.is_empty() {
                println!("No NASDAQ trade halts found.");
            } else {
                for halt in halts {
                    println!("Ticker: {}", halt.ticker);
                    println!("Company: {}", halt.company_name);
                    println!("Market: {}", halt.market);
                    println!("Halt date: {}", halt.halt_date);
                    println!("Halt time: {}", halt.halt_time);
                    println!("Reason: {}", halt.reason);
                    println!("Resumption date: {:?}", halt.resumption_date);
                    println!("Resumption quote time: {:?}", halt.resumption_quote_time);
                    println!("Resumption trade time: {:?}", halt.resumption_trade_time);
                    println!("Pause threshold price: {:?}", halt.pause_threshold_price);
                    println!();
                }
            }
        }
        Ok(FetchResult::GlobeNewswire(releases)) => {
            if releases.is_empty() {
                println!("No GlobeNewswire releases found.");
            } else {
                for release in releases {
                    println!("Title: {}", release.title);
                    println!("Feed: {}", release.feed_name);
                    println!("Published: {:?}", release.pub_date);
                    println!("Link: {}", release.link);
                    println!("Description: {:?}", release.description);
                    println!("Categories: {:?}", release.categories);
                    println!();
                }
            }
        }
        Ok(FetchResult::GoogleNews(articles)) => {
            if articles.is_empty() {
                println!("No Google News articles found.");
            } else {
                for article in articles {
                    println!("Title: {}", article.title);
                    println!("Published: {:?}", article.pub_date);
                    println!("Link: {}", article.link);
                    println!("Description: {:?}", article.description);
                    println!(
                        "Sentiment score: {:.2}",
                        sentiment_analysis(&article.description.clone().unwrap_or_default()).await
                    );
                    println!();
                }
            }
        }
        Err(e) => eprintln!("Error fetching market: {}", e),
    }
}
