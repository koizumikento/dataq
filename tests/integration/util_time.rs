use dataq::util::time::normalize_rfc3339_utc;

#[test]
fn normalizes_offset_datetime_to_utc() {
    let normalized = normalize_rfc3339_utc("2026-02-23T20:15:30+09:00").expect("valid datetime");
    assert_eq!(normalized, "2026-02-23T11:15:30Z");
}

#[test]
fn keeps_utc_datetime() {
    let normalized = normalize_rfc3339_utc("2026-02-23T11:15:30Z").expect("valid datetime");
    assert_eq!(normalized, "2026-02-23T11:15:30Z");
}

#[test]
fn preserves_fractional_seconds() {
    let normalized =
        normalize_rfc3339_utc("2026-02-23T20:15:30.123456+09:00").expect("valid datetime");
    assert_eq!(normalized, "2026-02-23T11:15:30.123456Z");
}

#[test]
fn invalid_datetime_returns_none() {
    let normalized = normalize_rfc3339_utc("not-a-datetime");
    assert!(normalized.is_none());
}
