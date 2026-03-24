use chrono::{DateTime, NaiveDate, Utc};

use crate::types::SecFiling;

pub fn normalize_ticker(value: &str) -> String {
    value.trim().trim_start_matches('$').to_ascii_uppercase()
}

pub fn parse_datetime_to_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc))
}

pub fn parse_filing_datetime(filing: &SecFiling) -> Option<DateTime<Utc>> {
    filing
        .acceptance_datetime
        .as_deref()
        .and_then(parse_datetime_to_utc)
        .or_else(|| {
            NaiveDate::parse_from_str(&filing.filing_date, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|datetime| datetime.and_utc())
        })
}
