use std::collections::{HashMap, HashSet};

use atom_syndication::{Entry, Feed};
use chrono::NaiveDate;
use reqwest::Client;
use serde::Deserialize;

use crate::{
    AppResult,
    throttle::{RequestSource, send_with_throttle},
    types::SecFiling,
    utils::normalize_ticker,
};

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
    #[serde(default)]
    files: Vec<SecHistoricalFile>,
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

#[derive(Debug, Deserialize)]
struct CompanyTickerEntry {
    ticker: String,
    cik_str: u64,
}

#[derive(Debug, Deserialize)]
struct SecHistoricalFile {
    name: String,
    #[serde(rename = "filingFrom", default)]
    filing_from: String,
    #[serde(rename = "filingTo", default)]
    filing_to: String,
}

pub async fn fetch_sec_edgar_all(identifier: &str, client: &Client) -> AppResult<Vec<SecFiling>> {
    let feed_url = identifier.to_string();

    let cik_to_ticker = load_company_ticker_entries(client)
        .await?
        .into_values()
        .map(|entry| (format!("{:010}", entry.cik_str), entry.ticker))
        .collect::<HashMap<_, _>>();

    let feed = fetch_atom_feed(client, &feed_url).await?;

    Ok(feed
        .entries()
        .iter()
        .filter_map(|entry| parse_atom_entry(entry, &cik_to_ticker))
        .collect())
}

pub async fn fetch_sec_edgar_ticker(ticker: &str, client: &Client) -> AppResult<Vec<SecFiling>> {
    fetch_sec_edgar_ticker_since(ticker, client, None).await
}

pub async fn fetch_sec_edgar_ticker_since(
    ticker: &str,
    client: &Client,
    min_date: Option<NaiveDate>,
) -> AppResult<Vec<SecFiling>> {
    let cik = lookup_sec_cik(ticker, client).await?;
    let submissions: SecSubmissions = send_with_throttle(
        client.get(format!("https://data.sec.gov/submissions/CIK{cik}.json")),
        RequestSource::SecEdgar,
    )
    .await?
    .error_for_status()?
    .json()
    .await?;

    let SecSubmissions {
        name: company_name,
        tickers,
        filings,
    } = submissions;
    let SecFilings { recent, files } = filings;
    let canonical_ticker = tickers
        .first()
        .cloned()
        .unwrap_or_else(|| normalize_ticker(ticker));
    let mut filings =
        build_submission_filings(&recent, &company_name, &canonical_ticker, &cik, min_date);

    for file in files {
        if historical_file_is_older_than(&file, min_date) {
            continue;
        }

        let historical: SecRecentFilings = send_with_throttle(
            client.get(format!("https://data.sec.gov/submissions/{}", file.name)),
            RequestSource::SecEdgar,
        )
        .await?
        .error_for_status()?
        .json()
        .await?;

        filings.extend(build_submission_filings(
            &historical,
            &company_name,
            &canonical_ticker,
            &cik,
            min_date,
        ));
    }

    let mut seen = HashSet::new();
    filings.retain(|filing| seen.insert(filing.accession_number.clone()));
    filings.sort_by(|left, right| {
        right
            .filing_date
            .cmp(&left.filing_date)
            .then_with(|| right.acceptance_datetime.cmp(&left.acceptance_datetime))
    });

    Ok(filings)
}

pub async fn lookup_sec_cik(ticker: &str, client: &Client) -> AppResult<String> {
    let wanted = normalize_ticker(ticker);

    load_company_ticker_entries(client)
        .await?
        .into_values()
        .find(|entry| entry.ticker.eq_ignore_ascii_case(&wanted))
        .map(|entry| format!("{:010}", entry.cik_str))
        .ok_or_else(|| format!("Ticker not found: {ticker}").into())
}

fn build_submission_filing(
    index: usize,
    form: &str,
    recent: &SecRecentFilings,
    company_name: &str,
    ticker: &str,
    cik: &str,
) -> Option<SecFiling> {
    if !is_relevant_form(form) {
        return None;
    }

    let accession_number = recent.accession_numbers.get(index)?.clone();
    let primary_document = recent.primary_documents.get(index)?.clone();

    if accession_number.is_empty() || primary_document.is_empty() {
        return None;
    }

    let accession_no_dashes = accession_number.replace('-', "");
    let filing_url = format!(
        "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
        cik.trim_start_matches('0'),
        accession_no_dashes,
        primary_document
    );

    Some(SecFiling {
        ticker: ticker.to_string(),
        company_name: company_name.to_string(),
        cik: cik.to_string(),
        form: form.to_string(),
        filing_date: recent.filing_dates.get(index).cloned().unwrap_or_default(),
        acceptance_datetime: non_empty_string(recent.acceptance_datetimes.get(index)),
        accession_number,
        primary_document,
        primary_doc_description: non_empty_string(recent.primary_doc_descriptions.get(index)),
        items: non_empty_string(recent.items.get(index)),
        is_inline_xbrl: recent.is_inline_xbrl.get(index).copied().unwrap_or(0) != 0,
        filing_url,
    })
}

fn build_submission_filings(
    filings: &SecRecentFilings,
    company_name: &str,
    ticker: &str,
    cik: &str,
    min_date: Option<NaiveDate>,
) -> Vec<SecFiling> {
    filings
        .forms
        .iter()
        .enumerate()
        .filter_map(|(index, form)| {
            build_submission_filing(index, form, filings, company_name, ticker, cik)
        })
        .filter(|filing| filing_matches_min_date(filing, min_date))
        .collect()
}

fn parse_atom_entry(entry: &Entry, cik_to_ticker: &HashMap<String, String>) -> Option<SecFiling> {
    let title = entry.title().to_string();
    let (form, rest) = title.split_once(" - ")?;
    let form = form.trim();

    if !is_relevant_form(form) {
        return None;
    }

    let company_start = rest.find(" (")?;
    let company_name = rest[..company_start].trim().to_string();
    let after_company = &rest[company_start + 2..];
    let cik_end = after_company.find(')')?;
    let cik = after_company[..cik_end].trim().to_string();
    let filing_url = entry
        .links()
        .first()
        .map(|link| link.href().to_string())
        .unwrap_or_default();

    let accession_number = filing_url
        .split('/')
        .find(|part| part.len() == 18 && part.chars().all(|char| char.is_ascii_digit()))
        .map(|part| format!("{}-{}-{}", &part[0..10], &part[10..12], &part[12..18]))
        .unwrap_or_default();

    Some(SecFiling {
        ticker: cik_to_ticker.get(&cik).cloned().unwrap_or_default(),
        company_name,
        cik,
        form: form.to_string(),
        filing_date: entry.updated().date_naive().to_string(),
        acceptance_datetime: Some(entry.updated().to_rfc3339()),
        accession_number,
        primary_document: filing_url.rsplit('/').next().unwrap_or("").to_string(),
        primary_doc_description: entry
            .summary()
            .map(|summary| summary.value.to_string())
            .filter(|value| !value.trim().is_empty()),
        items: None,
        is_inline_xbrl: false,
        filing_url,
    })
}

fn is_relevant_form(form: &str) -> bool {
    return matches!(
        form,
        "8-K"
            | "8-K/A"
            | "6-K"
            | "6-K/A"
            | "20-F"
            | "20-F/A"
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

async fn load_company_ticker_entries(
    client: &Client,
) -> AppResult<HashMap<String, CompanyTickerEntry>> {
    Ok(send_with_throttle(
        client.get("https://www.sec.gov/files/company_tickers.json"),
        RequestSource::SecEdgar,
    )
    .await?
    .error_for_status()?
    .json()
    .await?)
}

async fn fetch_atom_feed(client: &Client, url: &str) -> AppResult<Feed> {
    let bytes = send_with_throttle(client.get(url), RequestSource::SecEdgar)
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    Ok(Feed::read_from(&bytes[..])?)
}

fn non_empty_string(value: Option<&String>) -> Option<String> {
    value.cloned().filter(|value| !value.trim().is_empty())
}

fn filing_matches_min_date(filing: &SecFiling, min_date: Option<NaiveDate>) -> bool {
    match min_date {
        Some(min_date) => NaiveDate::parse_from_str(&filing.filing_date, "%Y-%m-%d")
            .map(|date| date >= min_date)
            .unwrap_or(true),
        None => true,
    }
}

fn historical_file_is_older_than(file: &SecHistoricalFile, min_date: Option<NaiveDate>) -> bool {
    let Some(min_date) = min_date else {
        return false;
    };

    let file_to = NaiveDate::parse_from_str(&file.filing_to, "%Y-%m-%d").ok();
    let file_from = NaiveDate::parse_from_str(&file.filing_from, "%Y-%m-%d").ok();

    matches!(
        (file_from, file_to),
        (Some(file_from), Some(file_to)) if file_from < min_date && file_to < min_date
    )
}
