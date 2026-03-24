use reqwest::{
    Client,
    header::{
        ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, CONNECTION, HeaderMap, HeaderName, HeaderValue,
        UPGRADE_INSECURE_REQUESTS, USER_AGENT,
    },
};

const DEFAULT_SEC_COMPANY: &str = "TradingBot";
const DEFAULT_SEC_EMAIL: &str = "admin@example.com";

pub fn build_client() -> Client {
    let company = env_or_default("MagnusTradingBot", DEFAULT_SEC_COMPANY);
    let email = env_or_default("mgrini2003@gmail.com", DEFAULT_SEC_EMAIL);
    let user_agent = format!("{company} {email}");

    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        header_value(&user_agent, "TradingBot admin@example.com"),
    );
    headers.insert(
        HeaderName::from_static("from"),
        header_value(&email, DEFAULT_SEC_EMAIL),
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "application/json,application/atom+xml,application/xml;q=0.9,text/xml;q=0.9,text/html;q=0.8,*/*;q=0.7",
        ),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(UPGRADE_INSECURE_REQUESTS, HeaderValue::from_static("1"));

    Client::builder()
        .default_headers(headers)
        .cookie_store(true)
        .brotli(true)
        .gzip(true)
        .deflate(true)
        .build()
        .unwrap_or_else(|_| Client::new())
}

fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn header_value(value: &str, fallback: &'static str) -> HeaderValue {
    HeaderValue::from_str(value).unwrap_or_else(|_| HeaderValue::from_static(fallback))
}
