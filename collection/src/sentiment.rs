use std::collections::HashSet;

use crate::types::GoogleArticle;

#[derive(Clone, Copy, Debug, Default)]
struct SentimentTally {
    total_score: f64,
    matched_terms: f64,
}

impl SentimentTally {
    fn add(&mut self, score: f64) {
        self.total_score += score;
        self.matched_terms += 1.0;
    }

    fn normalized(self) -> Option<f64> {
        if self.matched_terms == 0.0 {
            return None;
        }

        let average_score = self.total_score / self.matched_terms;
        let confidence = 0.6 + (self.matched_terms / (self.matched_terms + 3.0)) * 0.4;
        Some((average_score * confidence).clamp(-1.0, 1.0))
    }
}

pub fn sentiment_analysis(text: &str) -> f64 {
    let normalized = text.to_ascii_lowercase();
    let tokens: Vec<&str> = normalized
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();

    if tokens.is_empty() {
        return 0.0;
    }

    clause_adjusted_sentiment(&tokens)
        .or_else(|| score_token_slice(&tokens).normalized())
        .unwrap_or(0.0)
}

pub fn average_article_sentiment(articles: &[GoogleArticle]) -> f64 {
    if articles.is_empty() {
        return 0.0;
    }

    let mut seen_headlines = HashSet::new();
    let mut total_sentiment = 0.0;
    let mut counted_articles = 0;

    for article in articles {
        let headline_key = normalize_headline_key(&article.title);
        if !seen_headlines.insert(headline_key) {
            continue;
        }

        total_sentiment += article_sentiment(article);
        counted_articles += 1;
    }

    if counted_articles == 0 {
        0.0
    } else {
        total_sentiment / counted_articles as f64
    }
}

fn matches_any<const N: usize>(token: &str, candidates: &[&str; N]) -> bool {
    candidates.contains(&token)
}

fn word_score(token: &str) -> Option<f64> {
    Some(match token {
        "acquire" | "acquires" | "acquired" | "approval" | "approvals" | "approve" | "approved"
        | "beat" | "beats" | "bullish" | "exceed" | "exceeds" | "exceeded" | "growth" | "grow"
        | "grows" | "grew" | "improve" | "improves" | "improved" | "outperform"
        | "outperformed" | "profit" | "profits" | "profitable" | "rebound" | "recovery"
        | "resilient" | "strong" | "surge" | "surges" | "surged" | "tailwind" | "upgrade"
        | "upgrades" | "upgraded" | "upside" | "undervalued" | "win" | "wins" | "won" => 1.0,
        "boost" | "boosts" | "gain" | "gains" | "gained" | "jump" | "jumps" | "jumped" | "lift"
        | "lifts" | "lifted" | "raise" | "raises" | "raised" => 0.8,
        "attractive" | "bought" | "buys" | "buying" | "confidence" | "demand" | "effective"
        | "effectiveness" | "leadership" | "momentum" | "strength" => 0.6,
        "bankruptcy" | "bankrupt" | "bearish" | "breach" | "concern" | "concerns" | "decline"
        | "declines" | "declined" | "delay" | "delays" | "delayed" | "downgrade" | "downgrades"
        | "downgraded" | "drop" | "drops" | "dropped" | "fall" | "falls" | "fell" | "fraud"
        | "headwind" | "headwinds" | "investigation" | "investigations" | "lawsuit"
        | "lawsuits" | "loss" | "losses" | "miss" | "misses" | "missed" | "plunge" | "plunges"
        | "plunged" | "probe" | "probes" | "recall" | "recalls" | "risk" | "risks" | "scandal"
        | "selloff" | "selloffs" | "slump" | "slumps" | "slumped" | "uncertainty" | "weak"
        | "weakness" | "warning" | "warnings" => -1.0,
        "antitrust" | "dilution" | "halt" | "halted" | "layoff" | "layoffs" | "offering"
        | "pressure" | "tested" | "testing" | "volatile" | "volatility" => -0.8,
        _ => return None,
    })
}

fn phrase_score(tokens: &[&str], index: usize) -> Option<(usize, f64)> {
    let first = tokens.get(index).copied()?;
    let second = tokens.get(index + 1).copied();
    let third = tokens.get(index + 2).copied();
    let fourth = tokens.get(index + 3).copied();

    match (first, second, third, fourth) {
        ("support", Some("level"), Some("being"), Some("tested")) => return Some((4, -1.6)),
        ("buys", Some(_), Some(_), Some("shares"))
        | ("buys", Some(_), Some("shares"), _)
        | ("bought", Some(_), Some(_), Some("shares"))
        | ("bought", Some(_), Some("shares"), _) => return Some((4, 1.2)),
        ("sells", Some(_), Some(_), Some("shares"))
        | ("sells", Some(_), Some("shares"), _)
        | ("sold", Some(_), Some(_), Some("shares"))
        | ("sold", Some(_), Some("shares"), _) => return Some((4, -1.2)),
        _ => {}
    }

    match (first, second, third) {
        ("price", Some("target"), Some(third))
            if matches_any(third, &["raised", "boosted", "hiked"]) =>
        {
            return Some((3, 1.6));
        }
        ("price", Some("target"), Some(third))
            if matches_any(third, &["cut", "lowered", "reduced"]) =>
        {
            return Some((3, -1.6));
        }
        (first, Some(_), Some("target"))
            if matches_any(
                first,
                &[
                    "raise", "raises", "raised", "boost", "boosts", "hike", "hikes",
                ],
            ) =>
        {
            return Some((3, 1.5));
        }
        (first, Some(_), Some("target"))
            if matches_any(
                first,
                &[
                    "cut", "cuts", "lower", "lowers", "lowered", "reduce", "reduces",
                ],
            ) =>
        {
            return Some((3, -1.5));
        }
        ("high", Some(_), Some("demand")) => return Some((3, 1.3)),
        ("largest", Some("weekly"), Some("decline")) => return Some((3, -1.7)),
        ("class", Some("action"), Some("lawsuit")) => return Some((3, -1.9)),
        ("fda", Some("grants"), Some("approval")) => return Some((3, 1.9)),
        ("sec", Some("opens"), Some("probe"))
        | ("sec", Some("launches"), Some("probe"))
        | ("sec", Some("begins"), Some("probe")) => return Some((3, -2.0)),
        _ => {}
    }

    match (first, second) {
        (first, Some(second))
            if matches_any(first, &["beat", "beats", "exceed", "exceeds", "exceeded"])
                && matches_any(second, &["expectations", "estimates", "forecast"]) =>
        {
            Some((2, 1.7))
        }
        (first, Some(second))
            if matches_any(first, &["miss", "misses", "missed"])
                && matches_any(second, &["expectations", "estimates", "forecast"]) =>
        {
            Some((2, -1.7))
        }
        (first, Some(second))
            if matches_any(first, &["raise", "raises", "raised", "boost", "boosts"])
                && matches_any(second, &["guidance", "outlook", "forecast", "dividend"]) =>
        {
            Some((2, 1.7))
        }
        (first, Some(second))
            if matches_any(
                first,
                &["cut", "cuts", "cutting", "lower", "lowers", "lowered"],
            ) && matches_any(second, &["guidance", "outlook", "forecast", "dividend"]) =>
        {
            Some((2, -1.8))
        }
        (first, Some(second))
            if matches_any(first, &["guidance", "outlook", "forecast", "dividend"])
                && matches_any(second, &["raised", "boosted", "increased"]) =>
        {
            Some((2, 1.7))
        }
        (first, Some(second))
            if matches_any(first, &["guidance", "outlook", "forecast", "dividend"])
                && matches_any(second, &["cut", "cuts", "lowered", "suspended"]) =>
        {
            Some((2, -1.8))
        }
        (first, Some(second))
            if matches_any(first, &["earnings", "revenue", "sales"])
                && matches_any(second, &["beat", "beats", "surge", "surges", "growth"]) =>
        {
            Some((2, 1.7))
        }
        (first, Some(second))
            if matches_any(first, &["earnings", "revenue", "sales"])
                && matches_any(
                    second,
                    &["miss", "misses", "decline", "declines", "warning"],
                ) =>
        {
            Some((2, -1.7))
        }
        (first, Some(second))
            if matches_any(first, &["record", "strong"])
                && matches_any(second, &["revenue", "sales", "profit", "profits"]) =>
        {
            Some((2, 1.6))
        }
        (first, Some(second))
            if matches_any(first, &["fda", "drug"])
                && matches_any(second, &["approval", "approves", "approved"]) =>
        {
            Some((2, 1.8))
        }
        (first, Some(second))
            if matches_any(first, &["profit", "earnings", "sales"]) && second == "warning" =>
        {
            Some((2, -1.8))
        }
        (first, Some(second)) if matches_any(first, &["legal", "court"]) && second == "win" => {
            Some((2, 1.7))
        }
        (first, Some(second))
            if matches_any(first, &["margin", "margins"])
                && matches_any(second, &["expansion", "expand", "expanded"]) =>
        {
            Some((2, 1.5))
        }
        (first, Some(second))
            if matches_any(first, &["margin", "margins"]) && second == "pressure" =>
        {
            Some((2, -1.6))
        }
        (first, Some(second))
            if matches_any(first, &["share", "shares", "stock"])
                && matches_any(second, &["buyback", "repurchase"]) =>
        {
            Some((2, 1.6))
        }
        (first, Some(second))
            if matches_any(
                first,
                &["share", "shares", "stock", "secondary", "dilutive"],
            ) && second == "offering" =>
        {
            Some((2, -1.7))
        }
        (first, Some(second))
            if matches_any(first, &["sec", "regulatory", "criminal", "antitrust"])
                && matches_any(second, &["probe", "investigation", "lawsuit"]) =>
        {
            Some((2, -1.9))
        }
        (first, Some(second)) if matches_any(first, &["data", "cyber"]) && second == "breach" => {
            Some((2, -1.8))
        }
        (first, Some(second))
            if first == "bankruptcy" && matches_any(second, &["filing", "risk"]) =>
        {
            Some((2, -2.0))
        }
        (first, Some(second)) if matches_any(first, &["trade", "trading"]) && second == "halt" => {
            Some((2, -1.8))
        }
        (first, Some(second))
            if matches_any(first, &["stock", "shares"])
                && matches_any(
                    second,
                    &["jump", "jumps", "rise", "rises", "surge", "surges"],
                ) =>
        {
            Some((2, 1.8))
        }
        (first, Some(second))
            if matches_any(first, &["stock", "shares"])
                && matches_any(
                    second,
                    &[
                        "fall", "falls", "drop", "drops", "slump", "slumps", "plunge", "plunges",
                    ],
                ) =>
        {
            Some((2, -1.8))
        }
        (first, Some(second))
            if matches_any(first, &["double", "doubling"]) && second == "down" =>
        {
            Some((2, 1.2))
        }
        (first, Some(second)) if first == "gains" && second == "confidence" => Some((2, 1.3)),
        (first, Some(second)) if first == "pricing" && second == "power" => Some((2, 1.2)),
        (first, Some(second)) if first == "largest" && second == "position" => Some((2, 1.1)),
        (first, Some(second)) if first == "holdings" && second == "lowered" => Some((2, -1.0)),
        (first, Some(second)) if first == "holdings" && second == "raised" => Some((2, 1.0)),
        (first, Some(second)) if first == "buy" && second == "rating" => Some((2, 1.2)),
        (first, Some(second)) if first == "sell" && second == "rating" => Some((2, -1.2)),
        (first, Some(second)) if first == "top" && second == "pick" => Some((2, 1.3)),
        (first, Some(second)) if first == "trading" && second == "down" => Some((2, -1.5)),
        (first, Some(second)) if first == "sell" && second == "off" => Some((2, -1.7)),
        (first, Some(second)) if first == "best" && second == "performance" => Some((2, 1.4)),
        (first, Some(second)) if first == "high" && second == "demand" => Some((2, 1.3)),
        (first, Some(second)) if first == "screaming" && second == "buy" => Some((2, 1.9)),
        _ => None,
    }
}

fn is_negation(token: &str) -> bool {
    return matches!(
        token,
        "no" | "not" | "never" | "none" | "without" | "hardly" | "neither"
    );
}

fn intensity(token: &str) -> f64 {
    match token {
        "deeply" | "extremely" | "highly" | "materially" | "sharply" | "significantly"
        | "strongly" => 1.35,
        "very" => 1.2,
        "modestly" | "slightly" => 0.8,
        _ => 1.0,
    }
}

fn clause_weights(token: &str) -> Option<(f64, f64)> {
    match token {
        "but" | "however" | "though" | "although" | "yet" => Some((0.75, 1.35)),
        "despite" | "amid" => Some((1.35, 0.7)),
        "after" | "following" => Some((1.15, 0.9)),
        _ => None,
    }
}

fn is_negated(tokens: &[&str], index: usize) -> bool {
    (1..=3).any(|offset| index >= offset && is_negation(tokens[index - offset]))
}

fn score_token_slice(tokens: &[&str]) -> SentimentTally {
    let mut tally = SentimentTally::default();
    let mut index = 0;

    while index < tokens.len() {
        if let Some((phrase_len, mut score)) = phrase_score(tokens, index) {
            if index > 0 {
                score *= intensity(tokens[index - 1]);
            }

            if is_negated(tokens, index) {
                score *= -0.9;
            }

            tally.add(score);
            index += phrase_len;
            continue;
        }

        if let Some(mut score) = word_score(tokens[index]) {
            if index > 0 {
                score *= intensity(tokens[index - 1]);
            }

            if is_negated(tokens, index) {
                score *= -0.85;
            }

            tally.add(score);
        }

        index += 1;
    }

    tally
}

fn clause_adjusted_sentiment(tokens: &[&str]) -> Option<f64> {
    for (index, token) in tokens.iter().enumerate() {
        let Some((left_weight, right_weight)) = clause_weights(token) else {
            continue;
        };

        if index == 0 || index + 1 >= tokens.len() {
            continue;
        }

        let left_score = score_token_slice(&tokens[..index]).normalized();
        let right_score = score_token_slice(&tokens[index + 1..]).normalized();

        match (left_score, right_score) {
            (Some(left_score), Some(right_score)) => {
                let combined = (left_score * left_weight + right_score * right_weight)
                    / (left_weight + right_weight);
                return Some(combined.clamp(-1.0, 1.0));
            }
            (Some(score), None) | (None, Some(score)) => return Some(score),
            (None, None) => continue,
        }
    }

    None
}

fn normalize_headline_key(title: &str) -> String {
    title
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn article_sentiment(article: &GoogleArticle) -> f64 {
    let title_sentiment = sentiment_analysis(&article.title);
    let description_sentiment = article
        .description
        .as_deref()
        .map(sentiment_analysis)
        .unwrap_or(0.0);

    if description_sentiment == 0.0 {
        return title_sentiment;
    }

    if title_sentiment == 0.0 {
        return (description_sentiment * 0.65).clamp(-1.0, 1.0);
    }

    (title_sentiment * 0.75 + description_sentiment * 0.25).clamp(-1.0, 1.0)
}
