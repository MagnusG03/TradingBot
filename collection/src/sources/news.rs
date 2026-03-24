use chrono::Utc;
use chrono_tz::America::New_York;
use reqwest::{Client, RequestBuilder};
use rss::{Channel, Item};
use scraper::Html;

use crate::{
    AppResult,
    types::{GlobeNewswireRelease, GoogleArticle, NasdaqTradeHalt, PrNewswireRelease},
};

const GOOGLE_NEWS_REFERER: &str = "https://news.google.com/";

pub async fn fetch_prnewswire(
    client: &Client,
    feed_url: &str,
) -> AppResult<Vec<PrNewswireRelease>> {
    let channel = read_channel(client, feed_url, None).await?;
    let source_section = channel.title().to_string();

    Ok(channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = clean_title(item)?;
            Some(PrNewswireRelease {
                title,
                source_section: source_section.clone(),
                link: item.link().map(str::to_string),
                pub_date: item.pub_date().map(str::to_string),
                description: item.description().map(str::to_string),
            })
        })
        .collect())
}

pub async fn fetch_globenewswire(
    client: &Client,
    feed_url: &str,
) -> AppResult<Vec<GlobeNewswireRelease>> {
    let channel = read_channel(client, feed_url, None).await?;
    let feed_name = channel.title().trim().to_string();

    Ok(channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = clean_title(item)?;
            Some(GlobeNewswireRelease {
                feed_name: feed_name.clone(),
                title,
                link: item.link().unwrap_or_default().trim().to_string(),
                pub_date: item.pub_date().map(str::to_string),
                description: item.description().map(str::to_string),
                categories: item
                    .categories()
                    .iter()
                    .map(|category| category.name().trim().to_string())
                    .filter(|category| !category.is_empty())
                    .collect(),
            })
        })
        .collect())
}

pub async fn fetch_nasdaq_trade_halt(
    client: &Client,
    feed_url: &str,
) -> AppResult<Vec<NasdaqTradeHalt>> {
    let channel = read_channel(client, feed_url, None).await?;
    let today = Utc::now()
        .with_timezone(&New_York)
        .format("%m/%d/%Y")
        .to_string();

    Ok(channel
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
        .collect())
}

pub async fn fetch_google_news(url: &str, client: &Client) -> AppResult<Vec<GoogleArticle>> {
    let channel = read_channel(client, &url, Some(GOOGLE_NEWS_REFERER)).await?;
    let articles: Vec<GoogleArticle> = channel
        .items()
        .iter()
        .filter_map(|item| {
            let title = clean_title(item)?;
            Some(GoogleArticle {
                title,
                link: item.link().unwrap_or(url).trim().to_string(),
                pub_date: item.pub_date().map(|value| value.trim().to_string()),
                description: item
                    .description()
                    .map(clean_description)
                    .filter(|value| !value.is_empty()),
            })
        })
        .collect();

    Ok(articles)
}

async fn read_channel(client: &Client, url: &str, referer: Option<&str>) -> AppResult<Channel> {
    let request = add_referer(client.get(url), referer);
    let bytes = request.send().await?.error_for_status()?.bytes().await?;

    Ok(Channel::read_from(&bytes[..])?)
}

fn add_referer(request: RequestBuilder, referer: Option<&str>) -> RequestBuilder {
    match referer {
        Some(referer) => request.header("Referer", referer),
        None => request,
    }
}

fn clean_title(item: &Item) -> Option<String> {
    let title = item.title()?.trim();
    (!title.is_empty()).then(|| title.to_string())
}

fn clean_description(value: &str) -> String {
    Html::parse_fragment(value)
        .root_element()
        .text()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn get_ndaq_field(item: &Item, field: &str) -> Option<String> {
    item.extensions()
        .get("ndaq")
        .and_then(|fields| fields.get(field))
        .and_then(|values| values.first())
        .and_then(|ext| ext.value.clone())
}
