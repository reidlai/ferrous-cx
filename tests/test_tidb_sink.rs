//! Stories 23.1 & 23.2 (AD-36): TiDB 9-Table Olist DDL, `tidb_sink.toml`, and
//! Custom Apache Iggy -> TiDB Payload Transformation Verification.

use ferrous_cx::tidb::{
    normalize_mysql_datetime, parse_and_validate_tidb_sink_config, transform_iggy_payload,
    TidbRow, TIDB_SCHEMA_SQL,
};
use std::path::Path;

#[test]
fn test_story_23_1_tidb_schema_sql_contains_all_9_olist_and_cqrs_tables() {
    let required_tables = [
        "CREATE TABLE IF NOT EXISTS olist_orders",
        "CREATE TABLE IF NOT EXISTS olist_order_items",
        "CREATE TABLE IF NOT EXISTS olist_order_payments",
        "CREATE TABLE IF NOT EXISTS olist_order_reviews",
        "CREATE TABLE IF NOT EXISTS olist_customers",
        "CREATE TABLE IF NOT EXISTS olist_products",
        "CREATE TABLE IF NOT EXISTS olist_sellers",
        "CREATE TABLE IF NOT EXISTS olist_geolocation",
        "CREATE TABLE IF NOT EXISTS olist_category_translation",
        "CREATE TABLE IF NOT EXISTS orders_current_state",
        "CREATE TABLE IF NOT EXISTS orders_raw_events",
        "CREATE TABLE IF NOT EXISTS hpc_benchmarks",
        "CREATE TABLE IF NOT EXISTS _iggy_sink_offsets",
    ];
    for ddl in required_tables {
        assert!(
            TIDB_SCHEMA_SQL.contains(ddl),
            "Missing required TiDB DDL: {ddl}"
        );
    }
    // Verify exact Kaggle typo columns on olist_products
    assert!(TIDB_SCHEMA_SQL.contains("product_name_lenght"));
    assert!(TIDB_SCHEMA_SQL.contains("product_description_lenght"));
}

#[test]
fn test_story_23_2_tidb_sink_toml_parses_all_7_sinks() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cfg_path = manifest_dir.join("config/streaming/connectors/tidb_sink.toml");
    let raw = std::fs::read_to_string(&cfg_path).expect("read tidb_sink.toml");
    let cfg = parse_and_validate_tidb_sink_config(&raw).expect("valid tidb_sink.toml");

    assert_eq!(cfg.connector.name, "tidb_sink");
    assert_eq!(cfg.sinks.len(), 7);
    let tables: Vec<&str> = cfg.sinks.iter().map(|s| s.table.as_str()).collect();
    assert!(tables.contains(&"olist_orders"));
    assert!(tables.contains(&"olist_order_items"));
    assert!(tables.contains(&"olist_order_payments"));
    assert!(tables.contains(&"olist_order_reviews"));
    assert!(tables.contains(&"orders_current_state"));
    assert!(tables.contains(&"orders_raw_events"));
    assert!(tables.contains(&"hpc_benchmarks"));
}

#[test]
fn test_story_23_2_normalize_mysql_datetime_handles_rfc3339_and_olist_csv() {
    assert_eq!(
        normalize_mysql_datetime("2017-10-02T10:56:33Z").as_deref(),
        Some("2017-10-02 10:56:33.000000")
    );
    assert_eq!(
        normalize_mysql_datetime("2017-10-02 10:56:33").as_deref(),
        Some("2017-10-02 10:56:33.000000")
    );
    assert_eq!(normalize_mysql_datetime(""), None);
}

#[test]
fn test_story_23_2_transform_iggy_payload_all_7_tables() {
    // 1. olist_orders
    let ord = transform_iggy_payload(
        "olist_orders",
        &serde_json::json!({
            "order_id": "e481f51cbdc54678b7cc49136f2d6af7",
            "customer_id": "9ef432eb6251297304e76186b10a928d",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-10-02T10:56:33Z",
            "order_approved_at": "2017-10-02T11:07:15Z",
            "order_delivered_carrier_date": "2017-10-04T19:55:00Z",
            "order_delivered_customer_date": "2017-10-10T21:25:13Z",
            "order_estimated_delivery_date": "2017-10-18T00:00:00Z"
        }),
        100,
        200,
    )
    .expect("olist_orders transform");
    assert!(matches!(ord, TidbRow::OlistOrder { ref order_status, .. } if order_status == "delivered"));

    // 2. olist_order_items
    let item = transform_iggy_payload(
        "olist_order_items",
        &serde_json::json!({
            "order_id": "ord-1",
            "order_item_id": 2,
            "product_id": "prod-1",
            "seller_id": "sel-1",
            "shipping_limit_date": "2017-10-06 11:07:15",
            "price": 59.90,
            "freight_value": 12.50
        }),
        100,
        200,
    )
    .expect("olist_order_items transform");
    assert!(matches!(item, TidbRow::OlistOrderItem { order_item_id: 2, .. }));

    // 3. olist_order_payments
    let pay = transform_iggy_payload(
        "olist_order_payments",
        &serde_json::json!({
            "order_id": "ord-1",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 3,
            "payment_value": 72.40
        }),
        100,
        200,
    )
    .expect("olist_order_payments transform");
    assert!(matches!(pay, TidbRow::OlistOrderPayment { payment_installments: 3, .. }));

    // 4. olist_order_reviews
    let rev = transform_iggy_payload(
        "olist_order_reviews",
        &serde_json::json!({
            "review_id": "rev-1",
            "order_id": "ord-1",
            "review_score": 5,
            "review_comment_title": "Great",
            "review_comment_message": "Fast delivery",
            "review_creation_date": "2017-10-11T00:00:00Z",
            "review_answer_timestamp": "2017-10-12T08:00:00Z"
        }),
        100,
        200,
    )
    .expect("olist_order_reviews transform");
    assert!(matches!(rev, TidbRow::OlistOrderReview { review_score: 5, .. }));

    // 5. orders_current_state
    let state = transform_iggy_payload(
        "orders_current_state",
        &serde_json::json!({
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-10-02T10:56:33Z",
            "order_estimated_delivery_date": "2017-10-18T00:00:00Z",
            "total_amount_cents": 5990,
            "total_freight_cents": 1250,
            "courtesy_credit_cents": 500,
            "credit_delta": 500,
            "version": 2,
            "updated_at": "2017-10-12T10:00:00Z",
            "items": [{"order_item_id": 1, "product_id": "prod-1", "seller_id": "sel-1", "price_cents": 5990, "freight_cents": 1250}]
        }),
        100,
        200,
    )
    .expect("orders_current_state transform");
    assert!(matches!(
        state,
        TidbRow::OrderCurrentState {
            courtesy_credit_cents: 500,
            items_json: Some(_),
            ..
        }
    ));

    // 6. orders_raw_events
    let raw = transform_iggy_payload(
        "orders_raw_events",
        &serde_json::json!({
            "event_id": "evt-1",
            "order_id": "ord-1",
            "event_type": "CreditIssued"
        }),
        100,
        200,
    )
    .expect("orders_raw_events transform");
    assert!(matches!(raw, TidbRow::OrderRawEvent { ref event_id, .. } if event_id == "evt-1"));

    // 7. hpc_benchmarks
    let hpc = transform_iggy_payload(
        "hpc_benchmarks",
        &serde_json::json!({
            "run_id": "run-1",
            "record_id": "run-1:00000001",
            "seq": 1,
            "partition": 0,
            "client_timestamp_us": 50,
            "payload_size_bytes": 256,
            "data": "XXX"
        }),
        100,
        200,
    )
    .expect("hpc_benchmarks transform");
    assert!(matches!(hpc, TidbRow::HpcBenchmark { iggy_timestamp_us: 100, commit_timestamp_us: 200, .. }));
}
