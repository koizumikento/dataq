use chrono::{DateTime, SecondsFormat, Utc};

pub fn normalize_rfc3339_utc(input: &str) -> Option<String> {
    let dt = DateTime::parse_from_rfc3339(input).ok()?;
    Some(
        dt.with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )
}
