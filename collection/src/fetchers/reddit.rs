use reqwest::Client;
use serde_json::Value;

use crate::AppResult;

pub async fn fetch_reddit(url: &str, client: &Client, access_token: &str) -> AppResult<Value> {
    let post_id = url
        .split("/comments/")
        .nth(1)
        .and_then(|value| value.split('/').next())
        .ok_or("Could not extract Reddit post ID from URL")?;

    let api_url = format!("https://oauth.reddit.com/api/info?id=t3_{post_id}");

    Ok(client
        .get(api_url)
        .bearer_auth(access_token)
        .header("User-Agent", "MagnusTradingBot")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub async fn get_reddit_access_token(
    client: &Client,
    client_id: &str,
    client_secret: &str,
) -> AppResult<String> {
    let response: Value = client
        .post("https://ssl.reddit.com/api/v1/access_token")
        .basic_auth(client_id, Some(client_secret))
        .header("User-Agent", "MagnusTradingBot")
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    response
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| std::io::Error::other("Missing Reddit access token").into())
}
