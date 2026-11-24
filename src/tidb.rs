//! Pure-Rust TiDB Transactional HTAP Storage, Payload Transformer, Batch UPSERT Engine,
//! and 9-Table Olist Dimension Loader (Stories 23.1 & 23.2, AD-36).

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, MySqlPool, Pool};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

pub const DEFAULT_TIDB_URL: &str = "mysql://root@127.0.0.1:4000/ferrouscx";

/// Embedded canonical 9-table Kaggle Olist + CQRS + benchmark TiDB DDL (Story 23.1, AD-36).
pub const TIDB_SCHEMA_SQL: &str = include_str!("../config/tidb/schema.sql");

/// Parsed `tidb_sink.toml` connector configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TidbSinkConnectorConfig {
    pub connector: TidbConnectorMeta,
    pub iggy: TidbIggyMeta,
    pub sinks: Vec<TidbSinkDefinition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TidbConnectorMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    pub plugin: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TidbIggyMeta {
    pub server_address: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TidbSinkDefinition {
    pub name: String,
    pub stream: String,
    pub topic: String,
    pub schema: String,
    pub table: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

fn default_batch_size() -> usize {
    500
}

fn default_batch_timeout_ms() -> u64 {
    500
}

pub const SUPPORTED_STREAMED_TIDB_TABLES: &[&str] = &[
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "orders_current_state",
    "orders_raw_events",
    "hpc_benchmarks",
];

/// Parses and validates `tidb_sink.toml`.
pub fn parse_and_validate_tidb_sink_config(
    toml_str: &str,
) -> Result<TidbSinkConnectorConfig, String> {
    let parsed: TidbSinkConnectorConfig =
        toml::from_str(toml_str).map_err(|e| format!("Invalid tidb_sink.toml: {e}"))?;
    if parsed.connector.connector_type != "sink" {
        return Err(format!(
            "connector.type must be 'sink', got '{}'",
            parsed.connector.connector_type
        ));
    }
    if parsed.sinks.is_empty() {
        return Err("tidb_sink.toml must define at least one [[sinks]] entry".to_string());
    }
    for s in &parsed.sinks {
        if s.schema != "json" {
            return Err(format!(
                "Sink '{}' schema must be 'json', got '{}'",
                s.name, s.schema
            ));
        }
        if !SUPPORTED_STREAMED_TIDB_TABLES.contains(&s.table.as_str()) {
            return Err(format!(
                "Sink '{}' targets unsupported TiDB table '{}'",
                s.name, s.table
            ));
        }
        if s.batch_size == 0 || s.batch_timeout_ms == 0 {
            return Err(format!(
                "Sink '{}' batch_size and batch_timeout_ms must be > 0",
                s.name
            ));
        }
    }
    Ok(parsed)
}

/// Normalizes ISO-8601 / RFC3339 or Kaggle Olist timestamp strings into MySQL/TiDB `DATETIME(6)`
/// format (`YYYY-MM-DD HH:MM:SS.ffffff`).
pub fn normalize_mysql_datetime(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc).format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ndt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        }
    }
    None
}

fn now_mysql_datetime() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn json_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn json_i64(v: &serde_json::Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| {
        x.as_i64()
            .or_else(|| x.as_u64().map(|u| u as i64))
            .or_else(|| x.as_f64().map(|f| f as i64))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
    })
}

fn json_u64(v: &serde_json::Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_i64().and_then(|i| u64::try_from(i).ok()))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
    })
}

fn json_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| {
        x.as_f64()
            .or_else(|| x.as_i64().map(|i| i as f64))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
    })
}

/// Strongly typed transformed TiDB row ready for batched `INSERT ... ON DUPLICATE KEY UPDATE`.
#[derive(Debug, Clone, PartialEq)]
pub enum TidbRow {
    OlistOrder {
        order_id: String,
        customer_id: String,
        order_status: String,
        order_purchase_timestamp: String,
        order_approved_at: Option<String>,
        order_delivered_carrier_date: Option<String>,
        order_delivered_customer_date: Option<String>,
        order_estimated_delivery_date: String,
    },
    OlistOrderItem {
        order_id: String,
        order_item_id: i32,
        product_id: String,
        seller_id: String,
        shipping_limit_date: Option<String>,
        price: f64,
        freight_value: f64,
    },
    OlistOrderPayment {
        order_id: String,
        payment_sequential: i32,
        payment_type: String,
        payment_installments: i32,
        payment_value: f64,
    },
    OlistOrderReview {
        review_id: String,
        order_id: String,
        review_score: i32,
        review_comment_title: Option<String>,
        review_comment_message: Option<String>,
        review_creation_date: Option<String>,
        review_answer_timestamp: Option<String>,
    },
    OrderCurrentState {
        order_id: String,
        customer_id: String,
        order_status: String,
        order_purchase_timestamp: String,
        order_approved_at: Option<String>,
        order_delivered_carrier_date: Option<String>,
        order_delivered_customer_date: Option<String>,
        order_estimated_delivery_date: String,
        total_amount_cents: i64,
        total_freight_cents: i64,
        courtesy_credit_cents: i64,
        credit_delta: i64,
        version: u64,
        updated_at: String,
        items_json: Option<String>,
        payments_json: Option<String>,
    },
    OrderRawEvent {
        event_id: String,
        order_id: String,
        customer_id: String,
        event_type: String,
        payload: String,
        ingest_timestamp: String,
    },
    HpcBenchmark {
        run_id: String,
        record_id: String,
        seq: i64,
        partition_id: i32,
        client_timestamp_us: u64,
        iggy_timestamp_us: u64,
        commit_timestamp_us: u64,
        payload_size_bytes: i64,
        data: Option<String>,
    },
}

/// Transforms a raw Apache Iggy JSON payload into a strongly typed [`TidbRow`] for `table`.
pub fn transform_iggy_payload(
    table: &str,
    val: &serde_json::Value,
    iggy_ts_us: u64,
    commit_ts_us: u64,
) -> Result<TidbRow, String> {
    let now_dt = now_mysql_datetime();
    match table {
        "olist_orders" => {
            let order_id = json_str(val, "order_id").ok_or("olist_orders missing order_id")?;
            let customer_id = json_str(val, "customer_id").unwrap_or_default();
            let order_status = json_str(val, "order_status")
                .or_else(|| json_str(val, "status"))
                .unwrap_or_else(|| "created".to_string());
            let purchase_ts = json_str(val, "order_purchase_timestamp")
                .and_then(|s| normalize_mysql_datetime(&s))
                .unwrap_or_else(|| now_dt.clone());
            let est_ts = json_str(val, "order_estimated_delivery_date")
                .and_then(|s| normalize_mysql_datetime(&s))
                .unwrap_or_else(|| purchase_ts.clone());

            Ok(TidbRow::OlistOrder {
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp: purchase_ts,
                order_approved_at: json_str(val, "order_approved_at")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_delivered_carrier_date: json_str(val, "order_delivered_carrier_date")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_delivered_customer_date: json_str(val, "order_delivered_customer_date")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_estimated_delivery_date: est_ts,
            })
        }
        "olist_order_items" => {
            let order_id =
                json_str(val, "order_id").ok_or("olist_order_items missing order_id")?;
            let order_item_id = json_i64(val, "order_item_id").unwrap_or(1) as i32;
            let product_id = json_str(val, "product_id").unwrap_or_default();
            let seller_id = json_str(val, "seller_id").unwrap_or_default();
            Ok(TidbRow::OlistOrderItem {
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date: json_str(val, "shipping_limit_date")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                price: json_f64(val, "price").unwrap_or(0.0),
                freight_value: json_f64(val, "freight_value").unwrap_or(0.0),
            })
        }
        "olist_order_payments" => {
            let order_id =
                json_str(val, "order_id").ok_or("olist_order_payments missing order_id")?;
            Ok(TidbRow::OlistOrderPayment {
                order_id,
                payment_sequential: json_i64(val, "payment_sequential").unwrap_or(1) as i32,
                payment_type: json_str(val, "payment_type")
                    .unwrap_or_else(|| "not_defined".to_string()),
                payment_installments: json_i64(val, "payment_installments").unwrap_or(1) as i32,
                payment_value: json_f64(val, "payment_value").unwrap_or(0.0),
            })
        }
        "olist_order_reviews" => {
            let review_id =
                json_str(val, "review_id").ok_or("olist_order_reviews missing review_id")?;
            let order_id =
                json_str(val, "order_id").ok_or("olist_order_reviews missing order_id")?;
            Ok(TidbRow::OlistOrderReview {
                review_id,
                order_id,
                review_score: json_i64(val, "review_score").unwrap_or(5) as i32,
                review_comment_title: json_str(val, "review_comment_title"),
                review_comment_message: json_str(val, "review_comment_message"),
                review_creation_date: json_str(val, "review_creation_date")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                review_answer_timestamp: json_str(val, "review_answer_timestamp")
                    .and_then(|s| normalize_mysql_datetime(&s)),
            })
        }
        "orders_current_state" => {
            let order_id =
                json_str(val, "order_id").ok_or("orders_current_state missing order_id")?;
            let customer_id = json_str(val, "customer_id").unwrap_or_default();
            let order_status = json_str(val, "order_status")
                .or_else(|| json_str(val, "status"))
                .unwrap_or_else(|| "created".to_string());
            let purchase_ts = json_str(val, "order_purchase_timestamp")
                .and_then(|s| normalize_mysql_datetime(&s))
                .unwrap_or_else(|| now_dt.clone());
            let est_ts = json_str(val, "order_estimated_delivery_date")
                .or_else(|| json_str(val, "delivery_sla"))
                .and_then(|s| normalize_mysql_datetime(&s))
                .unwrap_or_else(|| purchase_ts.clone());
            let updated_at = json_str(val, "updated_at")
                .and_then(|s| normalize_mysql_datetime(&s))
                .unwrap_or_else(|| now_dt.clone());
            let courtesy = json_i64(val, "courtesy_credit_cents")
                .or_else(|| json_i64(val, "credit_issued"))
                .or_else(|| json_i64(val, "credit_issued_cents"))
                .unwrap_or(0);

            let items_json = val
                .get("items")
                .filter(|v| v.is_array() && !v.as_array().unwrap().is_empty())
                .and_then(|v| serde_json::to_string(v).ok());
            let payments_json = val
                .get("payments")
                .filter(|v| v.is_array() && !v.as_array().unwrap().is_empty())
                .and_then(|v| serde_json::to_string(v).ok());

            Ok(TidbRow::OrderCurrentState {
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp: purchase_ts,
                order_approved_at: json_str(val, "order_approved_at")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_delivered_carrier_date: json_str(val, "order_delivered_carrier_date")
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_delivered_customer_date: json_str(val, "order_delivered_customer_date")
                    .or_else(|| json_str(val, "actual_delivery"))
                    .and_then(|s| normalize_mysql_datetime(&s)),
                order_estimated_delivery_date: est_ts,
                total_amount_cents: json_i64(val, "total_amount_cents").unwrap_or(0),
                total_freight_cents: json_i64(val, "total_freight_cents").unwrap_or(0),
                courtesy_credit_cents: courtesy,
                credit_delta: json_i64(val, "credit_delta").unwrap_or(0),
                version: json_u64(val, "version").unwrap_or(0),
                updated_at,
                items_json,
                payments_json,
            })
        }
        "orders_raw_events" => {
            let event_id = json_str(val, "event_id")
                .or_else(|| json_str(val, "command_id"))
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let order_id = json_str(val, "order_id").unwrap_or_else(|| "unknown".to_string());
            let customer_id = json_str(val, "customer_id").unwrap_or_default();
            let event_type = json_str(val, "event_type").unwrap_or_else(|| "raw_event".to_string());
            let payload = serde_json::to_string(val).unwrap_or_else(|_| "{}".to_string());
            Ok(TidbRow::OrderRawEvent {
                event_id,
                order_id,
                customer_id,
                event_type,
                payload,
                ingest_timestamp: now_dt,
            })
        }
        "hpc_benchmarks" => {
            let run_id = json_str(val, "run_id").ok_or("hpc_benchmarks missing run_id")?;
            let record_id =
                json_str(val, "record_id").ok_or("hpc_benchmarks missing record_id")?;
            Ok(TidbRow::HpcBenchmark {
                run_id,
                record_id,
                seq: json_i64(val, "seq").unwrap_or(0),
                partition_id: json_i64(val, "partition").unwrap_or(0) as i32,
                client_timestamp_us: json_u64(val, "client_timestamp_us").unwrap_or(0),
                iggy_timestamp_us: iggy_ts_us,
                commit_timestamp_us: commit_ts_us,
                payload_size_bytes: json_i64(val, "payload_size_bytes").unwrap_or(0),
                data: json_str(val, "data"),
            })
        }
        other => Err(format!("Unsupported TiDB target table '{other}'")),
    }
}

/// Connects to the TiDB server without selecting a database, executes
/// `CREATE DATABASE IF NOT EXISTS <db>` (derived from `tidb_url`), and returns a connected pool.
pub async fn connect_and_init_tidb(tidb_url: &str, max_connections: u32) -> Result<MySqlPool, String> {
    if let Some((base_url, db_part)) = tidb_url.rsplit_once('/') {
        let db_name = db_part.split('?').next().unwrap_or("").trim();
        if !db_name.is_empty()
            && db_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            let server_url = format!("{base_url}/");
            if let Ok(admin_pool) = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(2)
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(&server_url)
                .await
            {
                let create_sql = format!("CREATE DATABASE IF NOT EXISTS `{db_name}`");
                let _ = sqlx::query(&create_sql).execute(&admin_pool).await;
                admin_pool.close().await;
            }
        }
    }

    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(tidb_url)
        .await
        .map_err(|e| format!("TiDB connect failed: {e}"))?;
    init_tidb_schema(&pool).await?;
    Ok(pool)
}

/// Applies the embedded canonical 9-table Olist + CQRS TiDB DDL idempotently.
pub async fn init_tidb_schema(pool: &Pool<MySql>) -> Result<(), String> {
    for raw_stmt in TIDB_SCHEMA_SQL.split(';') {
        let stmt: String = raw_stmt
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let trimmed = stmt.trim();
        if trimmed.is_empty() || trimmed.to_ascii_uppercase().starts_with("USE ") {
            continue;
        }
        sqlx::query(trimmed)
            .execute(pool)
            .await
            .map_err(|e| format!("DDL failed ({}): {e}", &trimmed[..trimmed.len().min(60)]))?;
    }
    Ok(())
}

/// Loads persisted Iggy partition offsets for `sink_name` from `_iggy_sink_offsets`.
pub async fn load_sink_offsets(
    pool: &MySqlPool,
    sink_name: &str,
) -> Result<HashMap<u32, u64>, String> {
    let rows: Vec<(u32, u64)> = sqlx::query_as(
        "SELECT partition_id, next_offset FROM _iggy_sink_offsets WHERE sink_name = ?",
    )
    .bind(sink_name)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("load_sink_offsets({sink_name}): {e}"))?;

    Ok(rows.into_iter().collect())
}

/// Commits a batch of transformed [`TidbRow`]s AND updated `_iggy_sink_offsets` inside a
/// single ACID TiDB transaction (`INSERT ... ON DUPLICATE KEY UPDATE`).
pub async fn upsert_tidb_batch(
    pool: &MySqlPool,
    sink_name: &str,
    rows: &[TidbRow],
    partition_offsets: &HashMap<u32, u64>,
) -> Result<usize, String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("TiDB begin tx failed: {e}"))?;

    for row in rows {
        match row {
            TidbRow::OlistOrder {
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
            } => {
                sqlx::query(
                    "INSERT INTO olist_orders \
                     (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, \
                      order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                     customer_id = VALUES(customer_id), order_status = VALUES(order_status), \
                     order_purchase_timestamp = VALUES(order_purchase_timestamp), \
                     order_approved_at = VALUES(order_approved_at), \
                     order_delivered_carrier_date = VALUES(order_delivered_carrier_date), \
                     order_delivered_customer_date = VALUES(order_delivered_customer_date), \
                     order_estimated_delivery_date = VALUES(order_estimated_delivery_date)",
                )
                .bind(order_id)
                .bind(customer_id)
                .bind(order_status)
                .bind(order_purchase_timestamp)
                .bind(order_approved_at)
                .bind(order_delivered_carrier_date)
                .bind(order_delivered_customer_date)
                .bind(order_estimated_delivery_date)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert olist_orders: {e}"))?;
            }
            TidbRow::OlistOrderItem {
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value,
            } => {
                sqlx::query(
                    "INSERT INTO olist_order_items \
                     (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                     product_id = VALUES(product_id), seller_id = VALUES(seller_id), \
                     shipping_limit_date = VALUES(shipping_limit_date), \
                     price = VALUES(price), freight_value = VALUES(freight_value)",
                )
                .bind(order_id)
                .bind(order_item_id)
                .bind(product_id)
                .bind(seller_id)
                .bind(shipping_limit_date)
                .bind(price)
                .bind(freight_value)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert olist_order_items: {e}"))?;
            }
            TidbRow::OlistOrderPayment {
                order_id,
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value,
            } => {
                sqlx::query(
                    "INSERT INTO olist_order_payments \
                     (order_id, payment_sequential, payment_type, payment_installments, payment_value) \
                     VALUES (?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                     payment_type = VALUES(payment_type), \
                     payment_installments = VALUES(payment_installments), \
                     payment_value = VALUES(payment_value)",
                )
                .bind(order_id)
                .bind(payment_sequential)
                .bind(payment_type)
                .bind(payment_installments)
                .bind(payment_value)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert olist_order_payments: {e}"))?;
            }
            TidbRow::OlistOrderReview {
                review_id,
                order_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_creation_date,
                review_answer_timestamp,
            } => {
                sqlx::query(
                    "INSERT INTO olist_order_reviews \
                     (review_id, order_id, review_score, review_comment_title, review_comment_message, \
                      review_creation_date, review_answer_timestamp) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                     review_score = VALUES(review_score), \
                     review_comment_title = VALUES(review_comment_title), \
                     review_comment_message = VALUES(review_comment_message), \
                     review_creation_date = VALUES(review_creation_date), \
                     review_answer_timestamp = VALUES(review_answer_timestamp)",
                )
                .bind(review_id)
                .bind(order_id)
                .bind(review_score)
                .bind(review_comment_title)
                .bind(review_comment_message)
                .bind(review_creation_date)
                .bind(review_answer_timestamp)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert olist_order_reviews: {e}"))?;
            }
            TidbRow::OrderCurrentState {
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                total_amount_cents,
                total_freight_cents,
                courtesy_credit_cents,
                credit_delta,
                version,
                updated_at,
                items_json,
                payments_json,
            } => {
                sqlx::query(
                    "INSERT INTO orders_current_state \
                     (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, \
                      order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, \
                      total_amount_cents, total_freight_cents, courtesy_credit_cents, credit_delta, \
                      version, updated_at, items_json, payments_json) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                     customer_id = VALUES(customer_id), order_status = VALUES(order_status), \
                     order_approved_at = COALESCE(VALUES(order_approved_at), order_approved_at), \
                     order_delivered_carrier_date = COALESCE(VALUES(order_delivered_carrier_date), order_delivered_carrier_date), \
                     order_delivered_customer_date = COALESCE(VALUES(order_delivered_customer_date), order_delivered_customer_date), \
                     total_amount_cents = VALUES(total_amount_cents), \
                     total_freight_cents = VALUES(total_freight_cents), \
                     courtesy_credit_cents = VALUES(courtesy_credit_cents), \
                     credit_delta = VALUES(credit_delta), \
                     version = VALUES(version), updated_at = VALUES(updated_at), \
                     items_json = COALESCE(VALUES(items_json), items_json), \
                     payments_json = COALESCE(VALUES(payments_json), payments_json)",
                )
                .bind(order_id)
                .bind(customer_id)
                .bind(order_status)
                .bind(order_purchase_timestamp)
                .bind(order_approved_at)
                .bind(order_delivered_carrier_date)
                .bind(order_delivered_customer_date)
                .bind(order_estimated_delivery_date)
                .bind(total_amount_cents)
                .bind(total_freight_cents)
                .bind(courtesy_credit_cents)
                .bind(credit_delta)
                .bind(version)
                .bind(updated_at)
                .bind(items_json)
                .bind(payments_json)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert orders_current_state: {e}"))?;
            }
            TidbRow::OrderRawEvent {
                event_id,
                order_id,
                customer_id,
                event_type,
                payload,
                ingest_timestamp,
            } => {
                sqlx::query(
                    "INSERT INTO orders_raw_events \
                     (event_id, order_id, customer_id, event_type, payload, ingest_timestamp) \
                     VALUES (?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE ingest_timestamp = VALUES(ingest_timestamp)",
                )
                .bind(event_id)
                .bind(order_id)
                .bind(customer_id)
                .bind(event_type)
                .bind(payload)
                .bind(ingest_timestamp)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert orders_raw_events: {e}"))?;
            }
            TidbRow::HpcBenchmark {
                run_id,
                record_id,
                seq,
                partition_id,
                client_timestamp_us,
                iggy_timestamp_us,
                commit_timestamp_us,
                payload_size_bytes,
                data,
            } => {
                sqlx::query(
                    "INSERT INTO hpc_benchmarks \
                     (run_id, record_id, seq, partition_id, client_timestamp_us, iggy_timestamp_us, \
                      commit_timestamp_us, payload_size_bytes, data) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE commit_timestamp_us = VALUES(commit_timestamp_us)",
                )
                .bind(run_id)
                .bind(record_id)
                .bind(seq)
                .bind(partition_id)
                .bind(client_timestamp_us)
                .bind(iggy_timestamp_us)
                .bind(commit_timestamp_us)
                .bind(payload_size_bytes)
                .bind(data)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("upsert hpc_benchmarks: {e}"))?;
            }
        }
    }

    let now_dt = now_mysql_datetime();
    for (&part_id, &next_off) in partition_offsets {
        sqlx::query(
            "INSERT INTO _iggy_sink_offsets (sink_name, partition_id, next_offset, updated_at) \
             VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE next_offset = VALUES(next_offset), updated_at = VALUES(updated_at)",
        )
        .bind(sink_name)
        .bind(part_id)
        .bind(next_off)
        .bind(&now_dt)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("upsert _iggy_sink_offsets: {e}"))?;
    }

    tx.commit()
        .await
        .map_err(|e| format!("TiDB commit tx failed: {e}"))?;
    Ok(rows.len())
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            '\r' => {}
            _ => current.push(c),
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// Batch-UPSERTs the 5 static Kaggle Olist dimension CSVs directly into TiDB (Story 23.1, AD-36).
pub async fn load_olist_dimensions_to_tidb(
    pool: &MySqlPool,
    input_dir: &Path,
) -> Result<Vec<(String, usize)>, String> {
    init_tidb_schema(pool).await?;
    let mut loaded = Vec::new();

    // 1. olist_customers
    let cust_csv = input_dir.join("olist_customers_dataset.csv");
    if cust_csv.exists() {
        let text = std::fs::read_to_string(&cust_csv).map_err(|e| e.to_string())?;
        let mut count = 0usize;
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
            let c = parse_csv_line(line);
            if c.len() < 5 {
                continue;
            }
            sqlx::query(
                "INSERT INTO olist_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) \
                 VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE customer_city = VALUES(customer_city), customer_state = VALUES(customer_state)",
            )
            .bind(&c[0]).bind(&c[1]).bind(&c[2]).bind(&c[3]).bind(&c[4])
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
            count += 1;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        loaded.push(("olist_customers".to_string(), count));
    }

    // 2. olist_products (preserving exact Kaggle column spelling: lenght)
    let prod_csv = input_dir.join("olist_products_dataset.csv");
    if prod_csv.exists() {
        let text = std::fs::read_to_string(&prod_csv).map_err(|e| e.to_string())?;
        let mut count = 0usize;
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
            let c = parse_csv_line(line);
            if c.len() < 9 {
                continue;
            }
            let opt_str = |s: &str| if s.is_empty() { None } else { Some(s.to_string()) };
            let opt_i32 = |s: &str| s.parse::<i32>().ok().or_else(|| s.parse::<f64>().ok().map(|f| f as i32));
            let opt_f64 = |s: &str| s.parse::<f64>().ok();
            sqlx::query(
                "INSERT INTO olist_products (product_id, product_category_name, product_name_lenght, product_description_lenght, \
                 product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE product_category_name = VALUES(product_category_name)",
            )
            .bind(&c[0]).bind(opt_str(&c[1])).bind(opt_i32(&c[2])).bind(opt_i32(&c[3]))
            .bind(opt_i32(&c[4])).bind(opt_f64(&c[5])).bind(opt_f64(&c[6])).bind(opt_f64(&c[7])).bind(opt_f64(&c[8]))
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
            count += 1;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        loaded.push(("olist_products".to_string(), count));
    }

    // 3. olist_sellers
    let sel_csv = input_dir.join("olist_sellers_dataset.csv");
    if sel_csv.exists() {
        let text = std::fs::read_to_string(&sel_csv).map_err(|e| e.to_string())?;
        let mut count = 0usize;
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
            let c = parse_csv_line(line);
            if c.len() < 4 {
                continue;
            }
            sqlx::query(
                "INSERT INTO olist_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state) \
                 VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE seller_city = VALUES(seller_city)",
            )
            .bind(&c[0]).bind(&c[1]).bind(&c[2]).bind(&c[3])
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
            count += 1;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        loaded.push(("olist_sellers".to_string(), count));
    }

    // 4. olist_geolocation (deduplicated & averaged per CEP prefix)
    let geo_csv = input_dir.join("olist_geolocation_dataset.csv");
    if geo_csv.exists() {
        let text = std::fs::read_to_string(&geo_csv).map_err(|e| e.to_string())?;
        let mut grouped: BTreeMap<String, (f64, f64, usize, String, String)> = BTreeMap::new();
        for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
            let c = parse_csv_line(line);
            if c.len() < 5 {
                continue;
            }
            let lat = c[1].parse::<f64>().unwrap_or(0.0);
            let lng = c[2].parse::<f64>().unwrap_or(0.0);
            grouped
                .entry(c[0].clone())
                .and_modify(|(sl, sg, cnt, _, _)| {
                    *sl += lat;
                    *sg += lng;
                    *cnt += 1;
                })
                .or_insert((lat, lng, 1, c[3].clone(), c[4].clone()));
        }
        let count = grouped.len();
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        for (zip, (sl, sg, cnt, city, state)) in grouped {
            sqlx::query(
                "INSERT INTO olist_geolocation (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) \
                 VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE geolocation_lat = VALUES(geolocation_lat), geolocation_lng = VALUES(geolocation_lng)",
            )
            .bind(zip).bind(sl / cnt as f64).bind(sg / cnt as f64).bind(city).bind(state)
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        loaded.push(("olist_geolocation".to_string(), count));
    }

    // 5. olist_category_translation
    let cat_csv = input_dir.join("product_category_name_translation.csv");
    if cat_csv.exists() {
        let text = std::fs::read_to_string(&cat_csv).map_err(|e| e.to_string())?;
        let mut count = 0usize;
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
            let c = parse_csv_line(line);
            if c.len() < 2 {
                continue;
            }
            sqlx::query(
                "INSERT INTO olist_category_translation (product_category_name, product_category_name_english) \
                 VALUES (?, ?) ON DUPLICATE KEY UPDATE product_category_name_english = VALUES(product_category_name_english)",
            )
            .bind(&c[0]).bind(&c[1])
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
            count += 1;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        loaded.push(("olist_category_translation".to_string(), count));
    }

    Ok(loaded)
}
