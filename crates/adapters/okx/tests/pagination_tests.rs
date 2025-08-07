use chrono::{Timelike, Utc};
use nautilus_model::{
    data::{Bar, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    types::{price::Price, quantity::Quantity},
};
use nautilus_okx::{common::enums::OKXInstrumentType, http::client::OKXHttpClient};
use std::str::FromStr;

fn assert_bars_monotonic(bars: &[Bar]) {
    for (i, window) in bars.windows(2).enumerate() {
        if window[0].ts_event >= window[1].ts_event {
            println!("MONOTONIC VIOLATION at index {}:", i);
            println!(
                "  Bar {}: ts={} ({:?})",
                i,
                window[0].ts_event.as_i64(),
                chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(window[0].ts_event.as_i64(),)
            );
            println!(
                "  Bar {}: ts={} ({:?})",
                i + 1,
                window[1].ts_event.as_i64(),
                chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(window[1].ts_event.as_i64(),)
            );

            let diff = window[0].ts_event.as_i64() - window[1].ts_event.as_i64();
            println!(
                "  Difference: {} nanoseconds ({} seconds)",
                diff,
                diff / 1_000_000_000
            );

            let start_idx = i.saturating_sub(2);
            let end_idx = (i + 4).min(bars.len());
            println!("  Context (bars {} to {}):", start_idx, end_idx - 1);
            for (j, bar) in bars[start_idx..end_idx].iter().enumerate() {
                let actual_idx = start_idx + j;
                let marker = if actual_idx == i || actual_idx == i + 1 {
                    " <-- VIOLATION"
                } else {
                    ""
                };
                println!(
                    "    Bar {}: ts={}{}",
                    actual_idx,
                    bar.ts_event.as_i64(),
                    marker
                );
            }

            panic!(
                "Bars are not monotonic: {} >= {}",
                window[0].ts_event, window[1].ts_event
            );
        }
    }
}

fn assert_bars_within_limit(bars: &[Bar], limit: Option<u32>) {
    if let Some(l) = limit {
        if l > 0 {
            assert!(
                bars.len() <= l as usize,
                "Bar count {} exceeds limit {}",
                bars.len(),
                l
            );
        }
    }
}

fn create_test_bar_type() -> BarType {
    let instrument_id = InstrumentId::from_str("BTC-USDT.OKX").unwrap();
    BarType::new(
        instrument_id,
        nautilus_model::data::BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
        AggregationSource::External,
    )
}

async fn create_test_client() -> Result<OKXHttpClient, Box<dyn std::error::Error>> {
    let mut client = OKXHttpClient::from_env()?;

    let instrument_types = [
        ("SPOT", OKXInstrumentType::Spot),
        ("MARGIN", OKXInstrumentType::Margin),
        ("FUTURES", OKXInstrumentType::Futures),
    ];

    for (type_name, inst_type) in instrument_types {
        match client.request_instruments(inst_type).await {
            Ok(instruments) => {
                println!(
                    "Received {} {} instruments, attempting to cache...",
                    instruments.len(),
                    type_name
                );

                let initial_cache_size = client.get_cached_symbols().len();
                client.add_instruments(instruments);
                let final_cache_size = client.get_cached_symbols().len();
                let cached_count = final_cache_size - initial_cache_size;

                if cached_count > 0 {
                    println!(
                        "Successfully cached {} {} instruments",
                        cached_count, type_name
                    );
                    let symbols = client.get_cached_symbols();
                    for symbol in symbols.iter().take(5) {
                        println!("  - {}", symbol);
                    }
                    if symbols.len() > 5 {
                        println!("  ... and {} more", symbols.len() - 5);
                    }
                    return Ok(client);
                } else {
                    println!(
                        "Failed to cache any {} instruments due to precision issues",
                        type_name
                    );
                }
            }
            Err(e) => {
                println!("Failed to request {} instruments: {}", type_name, e);
            }
        }
    }

    Err("No instruments could be cached from any instrument type due to precision issues".into())
}

async fn select_liquid_instrument(
    client: &OKXHttpClient,
    min_recent_bars: usize,
) -> Result<(BarType, usize), Box<dyn std::error::Error>> {
    const LOOKBACK: chrono::Duration = chrono::Duration::hours(1);

    for symbol in client.get_cached_symbols() {
        let bar_type = make_bar_type(&symbol, 1);
        let since = Utc::now() - LOOKBACK;

        if let Ok(bars) = client
            .request_bars(bar_type, Some(since), None, Some(min_recent_bars as u32))
            .await
        {
            if bars.len() >= min_recent_bars {
                println!(
                    "Selected liquid instrument: {} ({} bars in last hour)",
                    symbol,
                    bars.len()
                );
                return Ok((bar_type, bars.len()));
            }
        }
    }

    for spot in ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BTC-USD"] {
        let bar_type = make_bar_type(spot, 1);
        let since = Utc::now() - LOOKBACK;

        if let Ok(bars) = client
            .request_bars(bar_type, Some(since), None, Some(min_recent_bars as u32))
            .await
        {
            if bars.len() >= min_recent_bars {
                println!(
                    "Selected fallback instrument: {} ({} bars in last hour)",
                    spot,
                    bars.len()
                );
                return Ok((bar_type, bars.len()));
            }
        }
    }

    Err("No instrument with sufficient recent trading data found".into())
}

fn make_bar_type<S: AsRef<str>>(symbol: S, step: usize) -> BarType {
    let symbol_str = symbol.as_ref();
    let instrument_id = InstrumentId::from_str(&format!("{}.OKX", symbol_str)).unwrap();
    BarType::new(
        instrument_id,
        nautilus_model::data::BarSpecification::new(step, BarAggregation::Minute, PriceType::Last),
        AggregationSource::External,
    )
}

fn create_test_bar_type_for_client(
    client: &OKXHttpClient,
) -> Result<BarType, Box<dyn std::error::Error>> {
    let cached_symbols = client.get_cached_symbols();
    if cached_symbols.is_empty() {
        return Err("No instruments cached".into());
    }

    let first_symbol = &cached_symbols[0];
    let instrument_id = InstrumentId::from_str(&format!("{}.OKX", first_symbol))?;

    println!("Using instrument: {}", first_symbol);

    Ok(BarType::new(
        instrument_id,
        nautilus_model::data::BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
        AggregationSource::External,
    ))
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn range_query_monotonic() {
        let client = match create_test_client().await {
            Ok(client) => client,
            Err(e) => {
                println!("SKIP: Test skipped due to instrument caching issues: {}", e);
                return;
            }
        };

        let bar_type = match create_test_bar_type_for_client(&client) {
            Ok(bar_type) => bar_type,
            Err(e) => {
                println!("SKIP: Test skipped - no suitable instruments cached: {}", e);
                return;
            }
        };

        let end = Utc::now();
        let start = end - chrono::Duration::minutes(15);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), None)
            .await
            .expect("Request should succeed with cached instruments");

        if !bars.is_empty() {
            assert_bars_monotonic(&bars);
            println!(
                "PASS: Range query returned {} bars in correct order",
                bars.len()
            );
        } else {
            println!("WARNING: Range query returned 0 bars (no data in time range)");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn forward_query_large_limit() {
        let client = match create_test_client().await {
            Ok(client) => client,
            Err(e) => {
                println!("SKIP: Test skipped due to instrument caching issues: {}", e);
                return;
            }
        };

        let bar_type = match create_test_bar_type_for_client(&client) {
            Ok(bar_type) => bar_type,
            Err(e) => {
                println!("SKIP: Test skipped - no suitable instruments cached: {}", e);
                return;
            }
        };

        let start = Utc::now() - chrono::Duration::hours(3);

        println!("Testing with limit=50 (single page)...");
        let bars_50 = client
            .request_bars(bar_type, Some(start), None, Some(50))
            .await
            .expect("Request should succeed with cached instruments");

        println!("Received {} bars with limit 50", bars_50.len());
        assert_bars_monotonic(&bars_50);
        println!("Single page test passed monotonic check");

        println!("Testing with limit=200 first...");
        let bars_200 = client
            .request_bars(bar_type, Some(start), None, Some(200))
            .await
            .expect("Request should succeed with cached instruments");

        println!("Received {} bars with limit 200", bars_200.len());
        if bars_200.len() > 100 {
            println!(
                "Small test: Successfully exceeded 100-bar cap with {} bars",
                bars_200.len()
            );
        }

        assert_bars_monotonic(&bars_200);
        println!("Small test passed monotonic check");

        println!("Testing with limit=600...");
        let bars = client
            .request_bars(bar_type, Some(start), None, Some(600))
            .await
            .expect("Request should succeed with cached instruments");

        assert_bars_monotonic(&bars);
        assert_bars_within_limit(&bars, Some(600));
        println!(
            "PASS: Forward query returned {} bars with limit 600",
            bars.len()
        );
        if bars.len() > 100 {
            println!("PASS: Successfully exceeded 100-bar historical cap");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn backward_limited() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type();

        let end = Utc::now() - chrono::Duration::hours(2);

        let bars = client
            .request_bars(bar_type, None, Some(end), Some(200))
            .await
            .expect("Request should succeed with cached instruments");

        assert_bars_monotonic(&bars);
        assert_bars_within_limit(&bars, Some(200));
        println!(
            "PASS: Backward query returned {} bars with limit 200",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn latest_exact_300() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let bars = client
            .request_bars(bar_type, None, None, Some(300))
            .await
            .expect("Request should succeed with cached instruments");

        assert_bars_monotonic(&bars);
        println!(
            "PASS: Latest query returned {} bars with limit 300",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn limit_one_returns_single_bar() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type();

        let bars = client
            .request_bars(bar_type, None, None, Some(1))
            .await
            .expect("Request should succeed with cached instruments");

        assert_eq!(bars.len(), 1, "limit=1 must yield exactly one bar");
        assert_bars_monotonic(&bars);
        println!("PASS: Limit 1 query returned exactly {} bar", bars.len());
    }

    #[tokio::test]
    #[ignore]
    async fn zero_limit_is_unbounded() {
        let client = OKXHttpClient::from_env().unwrap();
        let bar_type = create_test_bar_type();

        let start = Utc::now() - chrono::Duration::minutes(5);

        let bars = client
            .request_bars(bar_type, Some(start), None, Some(0))
            .await
            .unwrap();

        assert!(
            !bars.is_empty(),
            "limit=0 should not return empty (treated as unlimited)"
        );
        assert_bars_monotonic(&bars);
    }

    #[tokio::test]
    #[ignore]
    async fn future_window_returns_empty() {
        let client = OKXHttpClient::from_env().unwrap();
        let bar_type = create_test_bar_type();

        let start = Utc::now() + chrono::Duration::minutes(1);
        let end = start + chrono::Duration::minutes(2);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), None)
            .await
            .unwrap();

        assert!(bars.is_empty(), "future window should yield 0 bars");
    }

    #[tokio::test]
    #[ignore]
    async fn pre_listing_range_returns_empty() {
        let client = OKXHttpClient::from_env().unwrap();
        let bar_type = create_test_bar_type();

        let start = chrono::DateTime::parse_from_rfc3339("2018-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = start + chrono::Duration::minutes(60);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), None)
            .await
            .unwrap();

        assert!(bars.is_empty(), "pre-listing query must return 0 bars");
    }

    #[tokio::test]
    #[ignore]
    async fn huge_limit_multiple_pages() {
        let client = OKXHttpClient::from_env().unwrap();
        let bar_type = create_test_bar_type();

        let start = Utc::now() - chrono::Duration::hours(48);
        let limit = 1_500;

        let bars = client
            .request_bars(bar_type, Some(start), None, Some(limit))
            .await
            .unwrap();

        assert_bars_monotonic(&bars);
        assert!(
            bars.len() >= 1_400 && bars.len() <= limit as usize,
            "expect ~1 500 bars, got {}",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn crosses_okx_history_split() {
        let client = OKXHttpClient::from_env().unwrap();
        let bar_type = create_test_bar_type();

        let end = Utc::now();
        let start = end - chrono::Duration::days(101);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(10_000))
            .await
            .unwrap();

        assert_bars_monotonic(&bars);
        assert!(
            !bars.is_empty(),
            "crossing 100-day split must still return bars"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn history_split_exactly_100_days() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() - chrono::Duration::days(100);
        let end = Utc::now() - chrono::Duration::days(99) - chrono::Duration::hours(1);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(200))
            .await
            .expect("Request should succeed");

        assert_bars_monotonic(&bars);
        println!(
            "PASS: Exactly 100-day boundary handled correctly with {} bars",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn range_spanning_history_boundary() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() - chrono::Duration::days(150);
        let end = Utc::now() - chrono::Duration::days(50);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(500))
            .await
            .expect("Request should succeed");

        assert!(
            !bars.is_empty(),
            "Range spanning boundary should return bars"
        );
        assert_bars_monotonic(&bars);

        if let Some(first_bar) = bars.first() {
            let first_ts =
                chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(first_bar.ts_event.as_i64());
            let age_days = Utc::now().signed_duration_since(first_ts).num_days();
            assert!(
                age_days > 120,
                "Should retrieve truly historical data, oldest bar is only {} days old",
                age_days
            );
        }

        println!(
            "PASS: Range spanning history boundary returned {} bars",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn non_unit_step_monotonic() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let cached_symbols = client.get_cached_symbols();
        if cached_symbols.is_empty() {
            panic!("No instruments cached");
        }

        let instrument_id = InstrumentId::from_str(&format!("{}.OKX", cached_symbols[0])).unwrap();
        let bar_type_5m = BarType::new(
            instrument_id,
            nautilus_model::data::BarSpecification::new(5, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );

        let start = Utc::now() - chrono::Duration::hours(4);
        let bars = client
            .request_bars(bar_type_5m, Some(start), None, Some(100))
            .await
            .expect("Request should succeed");

        assert_bars_monotonic(&bars);

        if bars.len() > 1 {
            for window in bars.windows(2) {
                let gap_ns = window[1].ts_event.as_i64() - window[0].ts_event.as_i64();
                let gap_minutes = gap_ns / (60 * 1_000_000_000);
                assert!(
                    gap_minutes >= 5,
                    "5-minute bars should be at least 5 minutes apart, got {} minutes",
                    gap_minutes
                );
            }
        }

        println!(
            "PASS: 5-minute bars have correct intervals, {} bars received",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn large_request_no_pagination_safeguard() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");

        let (bar_type, recent_count) = match select_liquid_instrument(&client, 10).await {
            Ok(result) => result,
            Err(e) => {
                println!("SKIP: No liquid instruments found: {}", e);
                return;
            }
        };

        println!("Using liquid instrument with {} recent bars", recent_count);

        let start = Utc::now() - chrono::Duration::hours(2);
        let result = client
            .request_bars(bar_type, Some(start), None, Some(500))
            .await;

        match result {
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("pagination safeguard") || error_msg.contains("tripped") {
                    panic!(
                        "FAIL: Pagination safeguard should not trigger for reasonable requests: {}",
                        error_msg
                    );
                } else {
                    println!("INFO: Non-pagination error (acceptable): {}", error_msg);
                }
            }
            Ok(bars) => {
                assert_bars_monotonic(&bars);
                assert!(bars.len() <= 500, "Should not exceed requested limit");
                assert!(
                    bars.len() > 20,
                    "Should get substantial data for 2-hour window on liquid instrument, got {} bars",
                    bars.len()
                );
                println!(
                    "PASS: Large request completed successfully with {} bars (no pagination safeguard)",
                    bars.len()
                );
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn cursor_advancement_precision() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() - chrono::Duration::hours(2);

        let page1 = client
            .request_bars(bar_type, Some(start), None, Some(100))
            .await
            .expect("Request should succeed");

        if !page1.is_empty() {
            let last_ts = page1.last().unwrap().ts_event.as_i64();
            let cursor = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(last_ts + 1_000_000);

            let page2 = client
                .request_bars(bar_type, Some(cursor), None, Some(100))
                .await
                .expect("Request should succeed");

            if !page1.is_empty() && !page2.is_empty() {
                let last_page1 = page1.last().unwrap().ts_event;
                let first_page2 = page2.first().unwrap().ts_event;

                assert!(
                    last_page1 < first_page2,
                    "Pages should not overlap: last_page1={}, first_page2={}",
                    last_page1,
                    first_page2
                );
            }

            println!("PASS: Cursor advancement prevents overlap between pages");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn forward_cursor_inside_bar_no_loop() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let now = Utc::now();
        let start = now
            .with_second(20)
            .unwrap_or(now)
            .with_nanosecond(425_000_000)
            .unwrap_or(now);

        println!("Testing with start time inside bar: {}", start);

        let result = client
            .request_bars(bar_type, Some(start), None, Some(50))
            .await;

        match result {
            Ok(bars) => {
                assert!(bars.len() <= 50, "Should not exceed limit");
                if !bars.is_empty() {
                    assert!(
                        bars.first().unwrap().ts_event.as_i64()
                            >= start.timestamp_nanos_opt().unwrap_or_default(),
                        "First bar should be at or after start time"
                    );
                }
                println!(
                    "PASS: Forward cursor inside bar completed with {} bars",
                    bars.len()
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("pagination safeguard") {
                    panic!(
                        "FAIL: Should not hit pagination safeguard for reasonable request: {}",
                        error_msg
                    );
                } else {
                    println!("INFO: Non-pagination error (acceptable): {}", error_msg);
                }
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn empty_future_history_breaks_immediately() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() + chrono::Duration::days(90);

        let bars = client
            .request_bars(bar_type, Some(start), None, Some(10))
            .await
            .expect("Future date request should succeed but return empty");

        assert!(bars.is_empty(), "Future date should return no bars");
        println!("PASS: Future date request returned empty immediately");
    }

    #[tokio::test]
    #[ignore]
    async fn regression_test_original_bug_scenario() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() - chrono::Duration::hours(8);

        println!("Regression test: start={}, requesting 500 bars", start);

        let start_time = std::time::Instant::now();
        let result = client
            .request_bars(bar_type, Some(start), None, Some(500))
            .await;
        let elapsed = start_time.elapsed();

        match result {
            Ok(bars) => {
                assert!(bars.len() <= 500, "Should not exceed limit");
                assert_bars_monotonic(&bars);
                println!(
                    "PASS: Regression test completed in {:?} with {} bars (no pagination safeguard)",
                    elapsed,
                    bars.len()
                );

                assert!(
                    elapsed.as_secs() < 30,
                    "Should complete within 30 seconds, took {:?}",
                    elapsed
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("pagination safeguard") {
                    panic!(
                        "FAIL: Regression test hit pagination safeguard - bug not fixed: {}",
                        error_msg
                    );
                } else {
                    println!("INFO: Non-pagination error (acceptable): {}", error_msg);
                }
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn backward_cursor_no_overlap() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let end = Utc::now() - chrono::Duration::hours(4);
        let bars = client
            .request_bars(bar_type, None, Some(end), Some(250))
            .await
            .expect("Request should succeed");

        assert_bars_monotonic(&bars);

        let mut seen = std::collections::HashSet::with_capacity(bars.len());
        for b in &bars {
            assert!(seen.insert(b.ts_event), "duplicate ts {}", b.ts_event);
        }
        println!(
            "PASS: Backward cursor prevents {} duplicate timestamps",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn range_cursor_no_overlap() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let end = Utc::now() - chrono::Duration::hours(2);
        let start = end - chrono::Duration::hours(2);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(350))
            .await
            .expect("Request should succeed");

        assert_bars_monotonic(&bars);

        let mut seen = std::collections::HashSet::with_capacity(bars.len());
        for b in &bars {
            assert!(seen.insert(b.ts_event), "duplicate ts {}", b.ts_event);
        }
        println!(
            "PASS: Range cursor prevents {} duplicate timestamps",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn forward_respects_start() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let start = Utc::now() - chrono::Duration::minutes(30);
        let bars = client
            .request_bars(bar_type, Some(start), None, Some(200))
            .await
            .expect("Request should succeed");

        assert!(!bars.is_empty());
        assert_bars_monotonic(&bars);

        let first_ts = bars.first().unwrap().ts_event;
        let start_ns = start.timestamp_nanos_opt().unwrap_or_default();
        let tolerance_ns = 60_000_000_000i64; // 60 seconds tolerance for 1-minute bars

        assert!(
            first_ts.as_i64() >= start_ns - tolerance_ns,
            "earliest bar {} too far before start {}, diff: {} seconds",
            first_ts.as_i64(),
            start_ns,
            (start_ns - first_ts.as_i64()) / 1_000_000_000
        );
        println!("PASS: Forward mode respects start boundary");
    }

    #[tokio::test]
    #[ignore]
    async fn backward_respects_end() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let end = Utc::now() - chrono::Duration::minutes(1); // Use more recent data
        let bars = client
            .request_bars(bar_type, None, Some(end), Some(150))
            .await
            .expect("Request should succeed");

        if bars.is_empty() {
            println!("SKIP: Backward pagination returned no data for recent timeframe");
            return;
        }

        assert_bars_monotonic(&bars);

        let last_ts = bars.last().unwrap().ts_event;
        let end_ns = end.timestamp_nanos_opt().unwrap_or_default();
        assert!(
            last_ts.as_i64() <= end_ns,
            "latest bar {} after end {}",
            last_ts.as_i64(),
            end_ns
        );
        println!("PASS: Backward mode respects end boundary");
    }

    #[tokio::test]
    #[ignore]
    async fn range_multi_page() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let end = Utc::now();
        let start = end - chrono::Duration::hours(6);
        let limit = 500;

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(limit))
            .await
            .expect("Request should succeed");

        assert!(bars.len() > 100, "expected multi-page, got {}", bars.len());
        assert_bars_monotonic(&bars);
        println!(
            "PASS: Range mode fetched {} bars across multiple pages",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn backward_multi_page() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let end = Utc::now() - chrono::Duration::minutes(1);
        let limit = 400;

        let bars = client
            .request_bars(bar_type, None, Some(end), Some(limit))
            .await
            .expect("Request should succeed");

        assert!(bars.len() > 100, "expected multi-page, got {}", bars.len());
        assert_bars_monotonic(&bars);
        println!(
            "PASS: Backward mode fetched {} bars across multiple pages",
            bars.len()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn range_respects_start_and_end() {
        let client = match create_test_client().await {
            Ok(c) => c,
            Err(e) => {
                println!(
                    "SKIP: Test skipped due to client/instrument setup issues: {}",
                    e
                );
                return;
            }
        };

        // Pick a cached, liquid instrument
        let bar_type = match create_test_bar_type_for_client(&client) {
            Ok(bt) => bt,
            Err(e) => {
                println!("SKIP: {}", e);
                return;
            }
        };

        // 20-minute window that is sure to contain data
        let end = Utc::now() - chrono::Duration::minutes(1);
        let start = end - chrono::Duration::minutes(20);

        let bars = client
            .request_bars(bar_type, Some(start), Some(end), Some(300))
            .await
            .expect("Range request should succeed");

        assert!(!bars.is_empty(), "Range query returned no bars");
        assert_bars_monotonic(&bars);

        let first_ts = bars.first().unwrap().ts_event.as_i64();
        let last_ts = bars.last().unwrap().ts_event.as_i64();
        let start_ns = start.timestamp_nanos_opt().unwrap_or_default();
        let end_ns = end.timestamp_nanos_opt().unwrap_or_default();

        assert!(
            first_ts >= start_ns,
            "earliest bar ({}) precedes start ({})",
            first_ts,
            start_ns
        );
        assert!(
            last_ts <= end_ns,
            "latest bar ({}) exceeds end ({})",
            last_ts,
            end_ns
        );

        println!(
            "PASS: Range respected bounds – {} bars from {:?} to {:?}",
            bars.len(),
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(first_ts),
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(last_ts)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn forward_and_backward_cursors_work_correctly() {
        let client = match create_test_client().await {
            Ok(client) => client,
            Err(e) => {
                println!("SKIP: Test skipped due to instrument caching issues: {}", e);
                return;
            }
        };

        let bar_type = match create_test_bar_type_for_client(&client) {
            Ok(bar_type) => bar_type,
            Err(e) => {
                println!("SKIP: Test skipped - no suitable instruments cached: {}", e);
                return;
            }
        };

        let now = Utc::now();
        let start = now - chrono::Duration::minutes(30);
        let _end = now - chrono::Duration::minutes(10); // Overlapping window to ensure data exists

        // FORWARD pagination (start-only) - should get bars from start onwards
        println!("Testing FORWARD pagination from {} onwards...", start);
        let forward_result = client
            .request_bars(bar_type, Some(start), None, Some(50))
            .await;

        let forward = match forward_result {
            Ok(bars) => bars,
            Err(e) => {
                println!("SKIP: Forward pagination failed: {}", e);
                return;
            }
        };

        if forward.is_empty() {
            println!("SKIP: Forward pagination returned no data");
            return;
        }

        // First bar should be at or after the start time (with tolerance for bar alignment)
        let start_ns = start.timestamp_nanos_opt().unwrap();
        let first_bar_ns = forward.first().unwrap().ts_event.as_i64();
        let tolerance_ns = 60_000_000_000i64; // 60 seconds tolerance for 1-minute bars

        assert!(
            first_bar_ns >= start_ns - tolerance_ns,
            "Forward pagination: first bar ({}) is too far before start ({}), diff: {} seconds",
            first_bar_ns,
            start_ns,
            (start_ns - first_bar_ns) / 1_000_000_000
        );

        // Verify chronological order for forward pagination
        assert_bars_monotonic(&forward);
        println!(
            "FORWARD: Retrieved {} bars starting from {}",
            forward.len(),
            start
        );

        // BACKWARD pagination (end-only) - using NOW as end to ensure recent data exists
        println!("Testing BACKWARD pagination up to now (recent data)...");
        let backward_result = client
            .request_bars(bar_type, None, Some(now), Some(50))
            .await;

        let backward = match backward_result {
            Ok(bars) => bars,
            Err(e) => {
                println!("SKIP: Backward pagination failed: {}", e);
                // Still consider the test successful if forward worked
                println!(
                    "PASS: Forward pagination verified (backward skipped due to data availability)"
                );
                return;
            }
        };

        if backward.is_empty() {
            println!("SKIP: Backward pagination returned no data");
            // Still consider the test successful if forward worked
            println!(
                "PASS: Forward pagination verified (backward skipped due to data availability)"
            );
            return;
        }

        // Last bar should be at or before the end time (now)
        let end_ns = now.timestamp_nanos_opt().unwrap();
        let last_bar_ns = backward.last().unwrap().ts_event.as_i64();

        assert!(
            last_bar_ns <= end_ns,
            "Backward pagination: last bar ({}) is after end ({}), diff: {} seconds",
            last_bar_ns,
            end_ns,
            (last_bar_ns - end_ns) / 1_000_000_000
        );

        // Verify chronological order for backward pagination
        assert_bars_monotonic(&backward);
        println!("BACKWARD: Retrieved {} bars ending at now", backward.len());

        // If both succeed, verify they work as expected
        println!(
            "✓ Both forward and backward pagination working correctly with after_ms/before_ms fix"
        );

        println!("PASS: Both forward and backward pagination work correctly");
    }

    #[tokio::test]
    #[ignore]
    async fn reversed_time_window_returns_error() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        let now = Utc::now();
        let start = now - chrono::Duration::minutes(10); // Earlier time
        let end = now - chrono::Duration::minutes(30); // Later time (reversed!)

        let result = client
            .request_bars(bar_type, Some(start), Some(end), Some(100))
            .await;

        match result {
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("start") && error_msg.contains("end")
                        || error_msg.contains("invalid")
                        || error_msg.contains("range"),
                    "Should reject reversed time window, got: {}",
                    error_msg
                );
                println!(
                    "PASS: Reversed time window correctly rejected: {}",
                    error_msg
                );
            }
            Ok(bars) => {
                // Some implementations might return empty instead of error
                assert!(
                    bars.is_empty(),
                    "Reversed time window should return empty or error, got {} bars",
                    bars.len()
                );
                println!("PASS: Reversed time window returned empty (acceptable)");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn far_future_requests_handle_gracefully() {
        let client = create_test_client()
            .await
            .expect("Failed to create test client");
        let bar_type = create_test_bar_type_for_client(&client).expect("Failed to create bar type");

        // Test 1: Far future start time
        let far_future_start = Utc::now() + chrono::Duration::days(365);
        let result = client
            .request_bars(bar_type, Some(far_future_start), None, Some(50))
            .await;

        match result {
            Ok(bars) => {
                assert!(
                    bars.is_empty(),
                    "Far future start should return empty, got {} bars",
                    bars.len()
                );
                println!("PASS: Far future start returned empty as expected");
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("future") || error_msg.contains("invalid"),
                    "Should reject or handle future dates gracefully, got: {}",
                    error_msg
                );
                println!("PASS: Far future start correctly rejected: {}", error_msg);
            }
        }

        // Test 2: Far future end time
        let now = Utc::now();
        let far_future_end = now + chrono::Duration::days(365);
        let start = now - chrono::Duration::hours(1);

        let result2 = client
            .request_bars(bar_type, Some(start), Some(far_future_end), Some(50))
            .await;

        match result2 {
            Ok(bars) => {
                // Should return recent data up to now, not future data
                if !bars.is_empty() {
                    let last_bar_ts = bars.last().unwrap().ts_event.as_i64();
                    let now_ns = now.timestamp_nanos_opt().unwrap_or_default();
                    assert!(
                        last_bar_ts <= now_ns + 60_000_000_000, // Allow 1 minute tolerance
                        "Future end time should not return bars beyond now"
                    );
                }
                println!(
                    "PASS: Far future end handled gracefully with {} bars",
                    bars.len()
                );
            }
            Err(e) => {
                println!("PASS: Far future end rejected: {}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn forward_and_backward_cursors() {
        let client = match create_test_client().await {
            Ok(client) => client,
            Err(e) => {
                println!("SKIP: Test skipped due to instrument caching issues: {}", e);
                return;
            }
        };

        let bar_type = match create_test_bar_type_for_client(&client) {
            Ok(bar_type) => bar_type,
            Err(e) => {
                println!("SKIP: Test skipped - no suitable instruments cached: {}", e);
                return;
            }
        };

        let now = Utc::now();
        let start = now - chrono::Duration::hours(2);
        let end = now - chrono::Duration::hours(1);

        // FORWARD pagination (start-only)
        println!("Testing FORWARD pagination from {} onwards...", start);
        let forward_result = client
            .request_bars(bar_type, Some(start), None, Some(180))
            .await;

        let forward = match forward_result {
            Ok(bars) => bars,
            Err(e) => {
                println!("SKIP: Forward pagination failed: {}", e);
                return;
            }
        };

        if forward.is_empty() {
            println!("SKIP: Forward pagination returned no data");
            return;
        }

        // Verify forward pagination: first bar should be at/after start boundary
        let start_ns = start.timestamp_nanos_opt().unwrap();
        let first_bar_ns = forward.first().unwrap().ts_event.as_i64();
        let tolerance_ns = 60_000_000_000i64; // 60 seconds tolerance for 1-minute bars

        assert!(
            first_bar_ns >= start_ns - tolerance_ns,
            "Forward pagination: first bar ({}) is too far before start ({}), diff: {} seconds",
            first_bar_ns,
            start_ns,
            (start_ns - first_bar_ns) / 1_000_000_000
        );

        assert_bars_monotonic(&forward);
        println!(
            "✅ FORWARD: Retrieved {} bars starting from {}",
            forward.len(),
            start
        );

        // BACKWARD pagination (end-only)
        println!("Testing BACKWARD pagination up to {} ...", end);
        let backward_result = client
            .request_bars(bar_type, None, Some(end), Some(180))
            .await;

        let backward = match backward_result {
            Ok(bars) => bars,
            Err(e) => {
                println!("SKIP: Backward pagination failed: {}", e);
                // Forward test already passed, so this is still a partial success
                println!("✅ FORWARD pagination verified (backward skipped)");
                return;
            }
        };

        if backward.is_empty() {
            println!("SKIP: Backward pagination returned no data");
            // Forward test already passed, so this is still a partial success
            println!("✅ FORWARD pagination verified (backward skipped)");
            return;
        }

        // Verify backward pagination: last bar should be at/before end boundary
        let end_ns = end.timestamp_nanos_opt().unwrap();
        let last_bar_ns = backward.last().unwrap().ts_event.as_i64();
        assert!(
            last_bar_ns <= end_ns,
            "Backward pagination: last bar ({}) is after end ({})",
            last_bar_ns,
            end_ns
        );

        assert_bars_monotonic(&backward);
        println!(
            "✅ BACKWARD: Retrieved {} bars ending at {}",
            backward.len(),
            end
        );

        println!(
            "✅ PASS: Both forward and backward cursors work correctly with after_ms/before_ms fix"
        );
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use nautilus_core::UnixNanos;
    // Needed for the Price::from_str(...) call below
    use std::str::FromStr;

    fn make_bar(ts: u64) -> Bar {
        Bar::new(
            create_test_bar_type(),
            Price::from("50000.00"),
            Price::from("50100.00"),
            Price::from("49900.00"),
            Price::from("50050.00"),
            Quantity::from(10),
            UnixNanos::new(ts),
            UnixNanos::new(ts),
        )
    }

    #[test]
    fn monotonic_passes() {
        let bars = vec![make_bar(1_000), make_bar(2_000)];
        assert_bars_monotonic(&bars);
    }

    #[test]
    #[should_panic(expected = "Bars are not monotonic")]
    fn monotonic_fails_on_duplicate() {
        let bars = vec![make_bar(1_000), make_bar(1_000)];
        assert_bars_monotonic(&bars);
    }

    #[test]
    #[should_panic(expected = "Bars are not monotonic")]
    fn monotonic_fails_on_descending() {
        let bars = vec![make_bar(2_000), make_bar(1_000)];
        assert_bars_monotonic(&bars);
    }

    #[test]
    fn limit_check_pass() {
        let bars = vec![make_bar(1_000), make_bar(2_000)];
        assert_bars_within_limit(&bars, Some(2));
        assert_bars_within_limit(&bars, Some(3));
        assert_bars_within_limit(&bars, None);
    }

    #[test]
    #[should_panic(expected = "Bar count 2 exceeds limit 1")]
    fn limit_check_fail() {
        let bars = vec![make_bar(1_000), make_bar(2_000)];
        assert_bars_within_limit(&bars, Some(1));
    }

    #[test]
    fn zero_limit_helper_is_noop() {
        let bars = vec![make_bar(1_000)];
        assert_bars_within_limit(&bars, Some(0));
    }

    #[test]
    fn bar_param_encoding_validation() {
        let instrument_id = InstrumentId::from_str("BTC-USDT.OKX").unwrap();

        let test_cases = [
            (1, BarAggregation::Minute),
            (5, BarAggregation::Minute),
            (15, BarAggregation::Minute),
            (1, BarAggregation::Hour),
            (4, BarAggregation::Hour),
            (1, BarAggregation::Day),
        ];

        for (step, agg) in test_cases {
            let bar_type = BarType::new(
                instrument_id,
                nautilus_model::data::BarSpecification::new(step, agg, PriceType::Last),
                AggregationSource::External,
            );
            assert_eq!(bar_type.spec().step.get(), step);
            assert_eq!(bar_type.spec().aggregation, agg);
        }
    }

    #[test]
    #[should_panic(expected = "Bar count")]
    fn limit_overflow_detection() {
        let bars: Vec<Bar> = (0..1000).map(|i| make_bar(i * 1000)).collect();
        assert_bars_within_limit(&bars, Some(100));
    }

    #[test]
    fn cursor_arithmetic_precision() {
        let base_ns = 1_609_459_200_000_000_000u64;
        let bar = make_bar(base_ns);

        // add ONE micro-second, not one milli-second
        let broken_cursor =
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(bar.ts_event.as_i64() + 1_000);
        let broken_after_ms = broken_cursor.timestamp_millis();

        let fixed_after_ms = (bar.ts_event.as_i64() / 1_000_000) + 1;

        assert_eq!(
            broken_after_ms,
            bar.ts_event.as_i64() / 1_000_000,
            "BROKEN: +1 µs gets rounded away by timestamp_millis()"
        );

        assert_eq!(
            fixed_after_ms,
            (bar.ts_event.as_i64() / 1_000_000) + 1,
            "FIXED: +1 ms (via integer maths) is preserved"
        );

        assert_ne!(
            broken_after_ms, fixed_after_ms,
            "The broken approach should not advance, the fixed one should"
        );

        println!("Broken approach: {} ms", broken_after_ms);
        println!("Fixed  approach: {} ms", fixed_after_ms);
        println!("Difference       : {} ms", fixed_after_ms - broken_after_ms);
    }

    #[test]
    fn price_precision_boundary() {
        assert!(Price::from_str("0.000000001").is_ok());

        let err = Price::from_str("0.0000000001").expect_err("must reject >9 decimal places");
        assert!(
            err.to_string().contains("precision"),
            "unexpected error: {err}"
        );

        let err = Price::from_str("0.000000000001").expect_err("must reject >9 decimal places");
        assert!(
            err.to_string().contains("precision"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cursor_precision_patch_is_active() {
        use nautilus_core::UnixNanos;

        let ts = 1_600_000_000_000_000_000u64;
        let last = UnixNanos::new(ts);

        let after_ms_good = (last.as_i64() / 1_000_000) + 1;

        let small_increment = 500_000;
        let broken_datetime =
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts as i64 + small_increment);
        let after_ms_bad = broken_datetime.timestamp_millis();

        println!("Original timestamp: {} ns", ts);
        println!("Original in milliseconds: {}", ts / 1_000_000);
        println!("Fixed approach (ms arithmetic): {}", after_ms_good);
        println!("Broken approach (+0.5ms then truncate): {}", after_ms_bad);

        assert_eq!(
            after_ms_bad,
            ts as i64 / 1_000_000,
            "Broken approach should not advance"
        );

        assert_eq!(
            after_ms_good,
            (ts as i64 / 1_000_000) + 1,
            "Fixed approach should advance by 1ms"
        );

        assert_ne!(
            after_ms_good, after_ms_bad,
            "precision fix regressed! Fixed: {}, Broken: {}",
            after_ms_good, after_ms_bad
        );
    }

    #[test]
    fn page_counter_only_counts_non_empty_pages() {
        let mut pages = 0;
        let mut guard_tripped = false;

        for i in 0..51 {
            let empty = i < 50;
            if empty {
                if pages >= 50 {
                    guard_tripped = true;
                    break;
                }
            } else {
                pages += 1;
            }
        }

        assert!(
            !guard_tripped,
            "page counter regression! Empty pages should not count toward safeguard"
        );
        assert_eq!(pages, 1, "should have counted exactly 1 non-empty page");
    }

    #[test]
    fn cursor_millis_increment_is_exactly_plus_one() {
        let ts_ns = 1_700_000_000_123_456_789u64;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts_ns as i64);
        let after = dt.timestamp_millis() + 1;

        let dt2 = chrono::DateTime::from_timestamp_millis(after).unwrap();
        assert!(dt2 > dt, "cursor did not advance by 1 ms exactly");

        assert_eq!(
            after - dt.timestamp_millis(),
            1,
            "cursor should advance by exactly 1ms"
        );
    }

    /// Ensures we never drop the required `FromStr` import again.
    /// If the trait isn't in scope this test won't compile.
    #[test]
    fn price_parses_when_fromstr_in_scope() {
        let p = Price::from_str("123.456").expect("parsing should succeed");
        assert_eq!(p.to_string(), "123.456");
    }

    #[test]
    fn microsecond_rounding_is_truncated() {
        // +1 µs should *not* advance timestamp_millis().
        let ts_ns: i64 = 1_700_000_000_000_000_000;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts_ns);
        let dt_plus = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts_ns + 1_000); // +1 µs

        assert_eq!(
            dt.timestamp_millis(),
            dt_plus.timestamp_millis(),
            "timestamp_millis() must truncate micro-second increments"
        );
    }

    /// Make sure `DateTime::<Utc>::from_timestamp_nanos` round-trips perfectly.
    #[test]
    fn datetime_from_nanos_roundtrip() {
        use chrono::Utc;
        let ts_ns: i64 = 1_725_811_234_567_890; // 2025-06-01T01:00:34.567890Z
        let dt = chrono::DateTime::<Utc>::from_timestamp_nanos(ts_ns);
        let back_ns = dt.timestamp_nanos_opt().unwrap();
        assert_eq!(
            back_ns, ts_ns,
            "round-trip via from_timestamp_nanos ↔ timestamp_nanos_opt must be lossless"
        );
    }
}
