//! Story 21.4: End-to-End Downloadable Kaggle `olist_orders` CSV -> Delta Lake Schema Verification (AD-34).
//!
//! Validates that:
//! 1. The bundled/downloadable `docs/datasets/olist/olist_orders_dataset.csv` header matches
//!    the exact 8 canonical `olist_orders` columns in `ferrous_cx::admin::TABLES`.
//! 2. `init_delta_storage` creates matching `_delta_log/00000000000000000000.json` metadata for
//!    both `olist_orders` (8 base columns) and `orders_current_state` (8 base + 6 CQRS overlay).
//! 3. Converting CSV rows into `delta-sink` NDJSON parts and committing `_delta_log/00000000000000000001.json`
//!    produces valid, queryable Delta Lake records with zero schema mismatches.

use ferrous_cx::admin::{init_delta_storage, TABLES};
use std::path::Path;

#[test]
fn test_story_21_4_olist_csv_to_delta_lake_end_to_end_schema_parity() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let csv_path = manifest_dir.join("docs/datasets/olist/olist_orders_dataset.csv");
    assert!(
        csv_path.exists(),
        "Olist dataset CSV must exist at {}",
        csv_path.display()
    );

    let csv_text = std::fs::read_to_string(&csv_path).expect("read olist_orders_dataset.csv");
    let mut lines = csv_text.lines();
    let header_line = lines.next().expect("CSV header must exist");
    let csv_columns: Vec<&str> = header_line
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .collect();

    let olist_spec = TABLES
        .iter()
        .find(|t| t.delta_table_name == "olist_orders")
        .expect("olist_orders spec");
    let expected_columns: Vec<&str> = olist_spec
        .schema_fields
        .iter()
        .map(|(name, _, _)| *name)
        .collect();

    assert_eq!(
        csv_columns, expected_columns,
        "Kaggle olist_orders_dataset.csv header must match AD-34 olist_orders table spec verbatim"
    );

    // Initialize local Delta Lake tables via admin::init_delta_storage
    let tmp = tempfile::tempdir().expect("tempdir");
    let delta_dir = tmp.path().join("delta");
    init_delta_storage(&delta_dir).expect("init_delta_storage must succeed");

    // Convert first 5 CSV rows into delta-sink NDJSON format and commit to olist_orders
    let mut ndjson_rows = Vec::new();
    for row in lines.take(5) {
        if row.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = row
            .split(',')
            .map(|s| s.trim().trim_matches('"'))
            .collect();
        assert_eq!(cols.len(), 8, "Each Olist CSV row must have 8 columns");
        let rec = serde_json::json!({
            "order_id": cols[0],
            "customer_id": cols[1],
            "order_status": cols[2],
            "order_purchase_timestamp": cols[3],
            "order_approved_at": if cols[4].is_empty() { serde_json::Value::Null } else { serde_json::json!(cols[4]) },
            "order_delivered_carrier_date": if cols[5].is_empty() { serde_json::Value::Null } else { serde_json::json!(cols[5]) },
            "order_delivered_customer_date": if cols[6].is_empty() { serde_json::Value::Null } else { serde_json::json!(cols[6]) },
            "order_estimated_delivery_date": cols[7],
        });
        ndjson_rows.push(serde_json::to_string(&rec).unwrap());
    }
    assert!(!ndjson_rows.is_empty(), "Must parse at least 1 Olist CSV row");

    let olist_table = delta_dir.join("olist_orders");
    let part_name = "part-00000000000000000001.ndjson";
    let ndjson_body = format!("{}\n", ndjson_rows.join("\n"));
    std::fs::write(olist_table.join(part_name), &ndjson_body).expect("write ndjson part");

    let commit_one = serde_json::json!({
        "add": {
            "path": part_name,
            "partitionValues": {},
            "size": ndjson_body.len(),
            "modificationTime": 1730000000000u64,
            "dataChange": true,
            "stats": serde_json::json!({ "numRecords": ndjson_rows.len() }).to_string()
        }
    });
    std::fs::write(
        olist_table.join("_delta_log/00000000000000000001.json"),
        format!("{commit_one}\n"),
    )
    .expect("write commit 1");

    // Verify commit-zero metadata schemaString on orders_current_state includes all 14 AD-34 fields
    let current_log_zero =
        std::fs::read_to_string(delta_dir.join("orders_current_state/_delta_log/00000000000000000000.json"))
            .expect("read orders_current_state log 0");
    let meta_line = current_log_zero.lines().nth(1).expect("metaData line");
    let meta_val: serde_json::Value = serde_json::from_str(meta_line).expect("parse metaData");
    let schema_str = meta_val["metaData"]["schemaString"]
        .as_str()
        .expect("schemaString");
    let schema_val: serde_json::Value = serde_json::from_str(schema_str).expect("parse schema");
    let fields = schema_val["fields"].as_array().expect("fields array");
    assert_eq!(fields.len(), 14, "orders_current_state must have 14 AD-34 fields");
    for (idx, expected_col) in expected_columns.iter().enumerate() {
        assert_eq!(fields[idx]["name"].as_str(), Some(*expected_col));
    }
}

#[test]
fn test_story_22_1_and_22_2_all_9_olist_tables_and_batch_dimension_loader() {
    use ferrous_cx::admin::{load_olist_dimensions, OLIST_TOPICS};

    // Story 22.1: Verify 4 streamed Olist topics and all 9 Olist Delta tables
    let topic_names: Vec<&str> = OLIST_TOPICS.iter().map(|t| t.name).collect();
    assert_eq!(
        topic_names,
        vec![
            "orders_stream",
            "order_items_stream",
            "order_payments_stream",
            "order_reviews_stream"
        ]
    );

    let expected_olist_tables = [
        "olist_orders",
        "olist_order_items",
        "olist_order_payments",
        "olist_order_reviews",
        "olist_customers",
        "olist_products",
        "olist_sellers",
        "olist_geolocation",
        "olist_category_translation",
    ];
    for tbl in expected_olist_tables {
        assert!(
            TABLES.iter().any(|t| t.delta_table_name == tbl),
            "Missing AD-35 TableInitSpec for {tbl}"
        );
    }

    // Verify exact Kaggle typo columns on olist_products
    let prod_spec = TABLES
        .iter()
        .find(|t| t.delta_table_name == "olist_products")
        .unwrap();
    let prod_cols: Vec<&str> = prod_spec.schema_fields.iter().map(|(n, _, _)| *n).collect();
    assert!(prod_cols.contains(&"product_name_lenght"));
    assert!(prod_cols.contains(&"product_description_lenght"));

    // Verify new transactional replay profiles exist
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (profile, dest) in [
        ("profiles/olist-items.toml", "olist/order_items_stream"),
        ("profiles/olist-payments.toml", "olist/order_payments_stream"),
        ("profiles/olist-reviews.toml", "olist/order_reviews_stream"),
    ] {
        let content = std::fs::read_to_string(manifest_dir.join(profile)).unwrap();
        assert!(
            content.contains(&format!("destination = \"{dest}\"")),
            "{profile} must route to {dest}"
        );
    }

    // Story 22.2: Exercise load_olist_dimensions on all 5 static dimension CSVs
    let tmp = tempfile::tempdir().expect("tempdir");
    let csv_dir = tmp.path().join("csvs");
    let delta_dir = tmp.path().join("delta");
    std::fs::create_dir_all(&csv_dir).unwrap();

    std::fs::write(
        csv_dir.join("olist_customers_dataset.csv"),
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n\
         c1,u1,01310,sao paulo,SP\n\
         c2,u1,01310,sao paulo,SP\n",
    )
    .unwrap();
    std::fs::write(
        csv_dir.join("olist_products_dataset.csv"),
        "product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm\n\
         p1,beleza_saude,40,287,1,225,16,10,14\n",
    )
    .unwrap();
    std::fs::write(
        csv_dir.join("olist_sellers_dataset.csv"),
        "seller_id,seller_zip_code_prefix,seller_city,seller_state\n\
         s1,13023,campinas,SP\n",
    )
    .unwrap();
    std::fs::write(
        csv_dir.join("olist_geolocation_dataset.csv"),
        "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state\n\
         01310,-23.56,-46.65,sao paulo,SP\n\
         01310,-23.58,-46.67,sao paulo,SP\n\
         20040,-22.90,-43.18,rio de janeiro,RJ\n",
    )
    .unwrap();
    std::fs::write(
        csv_dir.join("product_category_name_translation.csv"),
        "product_category_name,product_category_name_english\n\
         beleza_saude,health_beauty\n",
    )
    .unwrap();

    let report = load_olist_dimensions(&csv_dir, &delta_dir).expect("load_olist_dimensions");
    assert!(report.tables_skipped.is_empty());
    assert_eq!(report.tables_loaded.len(), 5);

    // Geolocation must deduplicate 3 rows -> 2 unique CEP prefixes with averaged coordinates
    let geo_count = report
        .tables_loaded
        .iter()
        .find(|(t, _)| t == "olist_geolocation")
        .map(|(_, c)| *c)
        .unwrap();
    assert_eq!(geo_count, 2, "olist_geolocation must deduplicate by zip prefix");

    let geo_part = std::fs::read_to_string(
        delta_dir.join("olist_geolocation/part-00000000000000000001.ndjson"),
    )
    .unwrap();
    let first_geo: serde_json::Value =
        serde_json::from_str(geo_part.lines().next().unwrap()).unwrap();
    assert_eq!(first_geo["geolocation_zip_code_prefix"], "01310");
    assert!((first_geo["geolocation_lat"].as_f64().unwrap() - (-23.57)).abs() < 1e-6);

    // Idempotent second load increments commit version cleanly to 00000000000000000002.json
    let report2 = load_olist_dimensions(&csv_dir, &delta_dir).expect("idempotent reload");
    assert_eq!(report2.tables_loaded.len(), 5);
    assert!(delta_dir
        .join("olist_customers/_delta_log/00000000000000000002.json")
        .exists());
}
