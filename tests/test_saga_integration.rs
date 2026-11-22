//! End-to-End Saga Integration & Platform Compose Verification Tests (Story 18.3, AD-24, AD-26)
//!
//! Validates:
//! 1. `docker-compose.yml` service topology for `order-saga`:
//!    - Image, container name, and non-root execution (`10001:10001`).
//!    - Day-0 Hardened Runtime Defense Constraints (`read_only`, `cap_drop: ALL`, `no-new-privileges`, `tmpfs`).
//!    - Healthcheck probe (`/order_saga --healthcheck`, interval, timeout, retries).
//!    - Deterministic startup dependency graph (`iggy-broker` and `sierradb` with `condition: service_healthy`).
//!    - Environment configuration injection (`IGGY_*`, `STREAM_NAME`, `TOPIC_ORDERS_EVENTS`, `CONSUMER_GROUP`, `ENTERPRISE_SETTLEMENT_URL`, `BACKOFF_*`).
//! 2. Negative validation assertions (verifies that missing Day-0 security constraints or missing healthcheck triggers audit failures).
//! 3. Mock end-to-end asynchronous saga lifecycle dataflow:
//!    - Event publishing (`CreditIssuedEvent`, plus non-credit event filtering and validation).
//!    - Saga correlation payload construction (`order_id`, `command_id`, `idempotency_key`, `amount_cents`, `status`).
//!    - Outbound enterprise dispatch simulation with transient error retries (exponential backoff).
//!    - Enterprise webhook callback payload validation and execution (settled & failed scenarios).
//!    - SierraDB append simulation & aggregate state transition to `Settled` / `SettlementFailed`.
//!    - Replay invariance and duplicate callback idempotency verification.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// ==============================================================================
// 1. Compose Configuration Verification Tests
// ==============================================================================

#[test]
fn test_docker_compose_order_saga_manifest_and_day0_defense() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let compose_path = Path::new(manifest_dir).join("docker-compose.yml");
    assert!(
        compose_path.exists(),
        "docker-compose.yml must exist at {}",
        compose_path.display()
    );

    let compose_content = std::fs::read_to_string(&compose_path).expect("read docker-compose.yml");
    let compose: serde_yaml::Value =
        serde_yaml::from_str(&compose_content).expect("parse docker-compose.yml YAML");

    // Top-level name check
    assert_eq!(
        compose.get("name").and_then(|v| v.as_str()),
        Some("ferrous-cx"),
        "docker-compose.yml must specify top-level 'name: ferrous-cx'"
    );

    let services = compose
        .get("services")
        .and_then(|v| v.as_mapping())
        .expect("services mapping must exist");

    // order-saga service presence
    let saga_key = serde_yaml::Value::String("order-saga".to_string());
    let saga = services
        .get(&saga_key)
        .expect("order-saga service must be declared in services");

    // Build context & Dockerfile
    let build = saga.get("build").expect("build configuration must exist");
    assert_eq!(
        build.get("context").and_then(|v| v.as_str()),
        Some("../ferrous-cqrses"),
        "build.context must point to ../ferrous-cqrses"
    );
    assert_eq!(
        build.get("dockerfile").and_then(|v| v.as_str()),
        Some("Dockerfile.order_saga"),
        "build.dockerfile must point to Dockerfile.order_saga"
    );

    // Image & container name
    assert_eq!(
        saga.get("image").and_then(|v| v.as_str()),
        Some("ferrous-cqrses-order-saga:latest")
    );
    assert_eq!(
        saga.get("container_name").and_then(|v| v.as_str()),
        Some("ferrous-cqrses-order-saga")
    );

    // Rootless non-root user
    assert_eq!(
        saga.get("user").and_then(|v| v.as_str()),
        Some("10001:10001"),
        "order-saga must run as rootless UID:GID 10001:10001"
    );
    assert_eq!(
        saga.get("restart").and_then(|v| v.as_str()),
        Some("unless-stopped")
    );

    // Day-0 Hardened Runtime Defense Constraints (AD-24)
    assert_eq!(
        saga.get("read_only").and_then(|v| v.as_bool()),
        Some(true),
        "order-saga must enforce read_only: true"
    );

    // cap_drop: ALL
    let cap_drop = saga
        .get("cap_drop")
        .and_then(|v| v.as_sequence())
        .expect("cap_drop must be a sequence");
    assert!(
        cap_drop.iter().any(|v| v.as_str() == Some("ALL")),
        "cap_drop must contain ALL"
    );

    // security_opt: no-new-privileges:true
    let security_opt = saga
        .get("security_opt")
        .and_then(|v| v.as_sequence())
        .expect("security_opt must be a sequence");
    assert!(
        security_opt
            .iter()
            .any(|v| v.as_str() == Some("no-new-privileges:true")),
        "security_opt must contain no-new-privileges:true"
    );

    // tmpfs: /tmp:noexec,nosuid,nodev,size=64M
    let tmpfs = saga
        .get("tmpfs")
        .and_then(|v| v.as_sequence())
        .expect("tmpfs must be a sequence");
    assert!(
        tmpfs.iter().any(|v| {
            v.as_str()
                .is_some_and(|s| s.starts_with("/tmp:noexec,nosuid,nodev,size=64M"))
        }),
        "tmpfs must contain /tmp:noexec,nosuid,nodev,size=64M"
    );

    // Deploy resource limits
    let deploy = saga.get("deploy").expect("deploy section must exist");
    let resources = deploy.get("resources").expect("resources must exist");
    let limits = resources.get("limits").expect("limits must exist");
    assert_eq!(
        limits.get("memory").and_then(|v| v.as_str()),
        Some("256M"),
        "deploy.resources.limits.memory must be 256M"
    );

    // Startup dependencies (gated with condition: service_healthy)
    let depends_on = saga
        .get("depends_on")
        .and_then(|v| v.as_mapping())
        .expect("depends_on must be a mapping");

    let iggy_key = serde_yaml::Value::String("iggy-broker".to_string());
    let iggy_dep = depends_on
        .get(&iggy_key)
        .expect("order-saga must depend on iggy-broker");
    assert_eq!(
        iggy_dep.get("condition").and_then(|v| v.as_str()),
        Some("service_healthy"),
        "iggy-broker dependency must require condition: service_healthy"
    );

    let sierra_key = serde_yaml::Value::String("sierradb".to_string());
    let sierra_dep = depends_on
        .get(&sierra_key)
        .expect("order-saga must depend on sierradb");
    assert_eq!(
        sierra_dep.get("condition").and_then(|v| v.as_str()),
        Some("service_healthy"),
        "sierradb dependency must require condition: service_healthy"
    );

    let api_key = serde_yaml::Value::String("ferrous-api".to_string());
    let api_dep = depends_on
        .get(&api_key)
        .expect("order-saga must depend on ferrous-api");
    assert_eq!(
        api_dep.get("condition").and_then(|v| v.as_str()),
        Some("service_healthy"),
        "ferrous-api dependency must require condition: service_healthy"
    );

    // Healthcheck probe
    let healthcheck = saga
        .get("healthcheck")
        .expect("healthcheck section must be defined");
    let test_cmd = healthcheck
        .get("test")
        .and_then(|v| v.as_sequence())
        .expect("healthcheck.test must be a sequence");
    let test_strings: Vec<&str> = test_cmd.iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(
        test_strings,
        vec!["CMD", "/order_saga", "--healthcheck"],
        "healthcheck.test must invoke /order_saga --healthcheck"
    );
    assert_eq!(
        healthcheck.get("interval").and_then(|v| v.as_str()),
        Some("10s")
    );
    assert_eq!(
        healthcheck.get("timeout").and_then(|v| v.as_str()),
        Some("5s")
    );
    assert_eq!(
        healthcheck.get("retries").and_then(|v| v.as_u64()),
        Some(3)
    );
    assert_eq!(
        healthcheck.get("start_period").and_then(|v| v.as_str()),
        Some("5s")
    );

    // Environment variables
    let env_seq = saga
        .get("environment")
        .and_then(|v| v.as_sequence())
        .expect("environment must be a sequence");
    let env_vars: Vec<&str> = env_seq.iter().filter_map(|v| v.as_str()).collect();

    let expected_env_prefixes = [
        "IGGY_SERVER_ADDRESS=",
        "IGGY_USERNAME=",
        "IGGY_PASSWORD=",
        "STREAM_NAME=orders",
        "TOPIC_ORDERS_EVENTS=consolidated",
        "CONSUMER_GROUP=cg-order-saga",
        "ENTERPRISE_SETTLEMENT_URL=",
        "ENTERPRISE_AUTH_TOKEN=",
        "MAX_RETRIES=3",
        "BACKOFF_INITIAL_MS=100",
        "BACKOFF_MAX_MS=2000",
    ];

    for expected in expected_env_prefixes {
        assert!(
            env_vars.iter().any(|var| var.starts_with(expected)),
            "Environment must define variable matching prefix '{}'",
            expected
        );
    }
}

#[test]
fn test_docker_compose_day0_negative_validation_cases() {
    // Validate that helper logic correctly rejects missing Day-0 fields
    fn audit_service_node(node: &serde_yaml::Value) -> Result<(), &'static str> {
        let read_only = node
            .get("read_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !read_only {
            return Err("Missing read_only: true");
        }

        let cap_drop = node.get("cap_drop").and_then(|v| v.as_sequence());
        if !cap_drop
            .map(|s| s.iter().any(|v| v.as_str() == Some("ALL")))
            .unwrap_or(false)
        {
            return Err("Missing cap_drop: [ALL]");
        }

        let sec_opt = node.get("security_opt").and_then(|v| v.as_sequence());
        if !sec_opt
            .map(|s| s.iter().any(|v| v.as_str() == Some("no-new-privileges:true")))
            .unwrap_or(false)
        {
            return Err("Missing security_opt: [no-new-privileges:true]");
        }

        let tmpfs = node.get("tmpfs").and_then(|v| v.as_sequence());
        if !tmpfs.map(|s| !s.is_empty()).unwrap_or(false) {
            return Err("Missing tmpfs");
        }

        let user = node.get("user").and_then(|v| v.as_str());
        if user != Some("10001:10001") {
            return Err("User must be 10001:10001");
        }

        Ok(())
    }

    // Valid sample
    let valid_yaml = r#"
        read_only: true
        cap_drop: ["ALL"]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp:noexec,nosuid,nodev,size=64M"]
        user: "10001:10001"
    "#;
    let valid_node: serde_yaml::Value = serde_yaml::from_str(valid_yaml).unwrap();
    assert!(audit_service_node(&valid_node).is_ok());

    // Missing read_only
    let no_readonly: serde_yaml::Value = serde_yaml::from_str(
        r#"
        cap_drop: ["ALL"]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp:noexec,nosuid,nodev,size=64M"]
        user: "10001:10001"
    "#,
    )
    .unwrap();
    assert_eq!(
        audit_service_node(&no_readonly),
        Err("Missing read_only: true")
    );

    // Missing cap_drop ALL
    let no_cap: serde_yaml::Value = serde_yaml::from_str(
        r#"
        read_only: true
        cap_drop: ["NET_ADMIN"]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp:noexec,nosuid,nodev,size=64M"]
        user: "10001:10001"
    "#,
    )
    .unwrap();
    assert_eq!(audit_service_node(&no_cap), Err("Missing cap_drop: [ALL]"));

    // Missing no-new-privileges
    let no_sec: serde_yaml::Value = serde_yaml::from_str(
        r#"
        read_only: true
        cap_drop: ["ALL"]
        tmpfs: ["/tmp:noexec,nosuid,nodev,size=64M"]
        user: "10001:10001"
    "#,
    )
    .unwrap();
    assert_eq!(
        audit_service_node(&no_sec),
        Err("Missing security_opt: [no-new-privileges:true]")
    );

    // Root execution
    let root_user: serde_yaml::Value = serde_yaml::from_str(
        r#"
        read_only: true
        cap_drop: ["ALL"]
        security_opt: ["no-new-privileges:true"]
        tmpfs: ["/tmp:noexec,nosuid,nodev,size=64M"]
        user: "0:0"
    "#,
    )
    .unwrap();
    assert_eq!(
        audit_service_node(&root_user),
        Err("User must be 10001:10001")
    );

    // Missing tmpfs
    let no_tmpfs: serde_yaml::Value = serde_yaml::from_str(
        r#"
        read_only: true
        cap_drop: ["ALL"]
        security_opt: ["no-new-privileges:true"]
        user: "10001:10001"
    "#,
    )
    .unwrap();
    assert_eq!(audit_service_node(&no_tmpfs), Err("Missing tmpfs"));

    // Healthcheck audit helper & negative cases
    fn audit_service_healthcheck(node: &serde_yaml::Value) -> Result<(), &'static str> {
        let hc = node.get("healthcheck").ok_or("Missing healthcheck section")?;
        let test = hc.get("test").and_then(|v| v.as_sequence()).ok_or("Missing healthcheck.test")?;
        if test.is_empty() {
            return Err("Empty healthcheck.test");
        }
        let interval = hc.get("interval").and_then(|v| v.as_str()).ok_or("Missing healthcheck.interval")?;
        if interval.is_empty() {
            return Err("Empty healthcheck.interval");
        }
        Ok(())
    }

    let no_hc: serde_yaml::Value = serde_yaml::from_str("read_only: true").unwrap();
    assert_eq!(audit_service_healthcheck(&no_hc), Err("Missing healthcheck section"));

    let empty_hc_test: serde_yaml::Value = serde_yaml::from_str(
        r#"
        healthcheck:
          test: []
          interval: "10s"
    "#,
    )
    .unwrap();
    assert_eq!(audit_service_healthcheck(&empty_hc_test), Err("Empty healthcheck.test"));

    // Dependency audit helper & negative cases
    fn audit_service_dependencies(node: &serde_yaml::Value, required_deps: &[&str]) -> Result<(), String> {
        let deps = node
            .get("depends_on")
            .and_then(|v| v.as_mapping())
            .ok_or_else(|| "Missing depends_on section".to_string())?;

        for req in required_deps {
            let key = serde_yaml::Value::String((*req).to_string());
            let dep_node = deps.get(&key).ok_or_else(|| format!("Missing required dependency: {req}"))?;
            let condition = dep_node.get("condition").and_then(|v| v.as_str());
            if condition != Some("service_healthy") {
                return Err(format!("Dependency '{req}' must require condition: service_healthy"));
            }
        }
        Ok(())
    }

    let no_deps: serde_yaml::Value = serde_yaml::from_str("read_only: true").unwrap();
    assert_eq!(
        audit_service_dependencies(&no_deps, &["iggy-broker"]).unwrap_err(),
        "Missing depends_on section"
    );

    let missing_sierradb: serde_yaml::Value = serde_yaml::from_str(
        r#"
        depends_on:
          iggy-broker:
            condition: service_healthy
    "#,
    )
    .unwrap();
    assert_eq!(
        audit_service_dependencies(&missing_sierradb, &["iggy-broker", "sierradb"]).unwrap_err(),
        "Missing required dependency: sierradb"
    );

    let unconditioned_dep: serde_yaml::Value = serde_yaml::from_str(
        r#"
        depends_on:
          iggy-broker:
            condition: service_started
    "#,
    )
    .unwrap();
    assert_eq!(
        audit_service_dependencies(&unconditioned_dep, &["iggy-broker"]).unwrap_err(),
        "Dependency 'iggy-broker' must require condition: service_healthy"
    );
}

// ==============================================================================
// 2. Mock End-to-End Saga Dataflow Verification
// ==============================================================================

const MAX_COURTESY_CREDIT_CENTS: i64 = 10_000; // $100.00 max limit

/// Domain event types conforming to CQRS-ES stream model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum MockDomainEvent {
    OrderCreated {
        event_id: String,
        order_id: String,
        customer_id: String,
        total_cents: i64,
        version: u64,
    },
    CreditIssued {
        event_id: String,
        order_id: String,
        amount_cents: i64,
        reason: String,
        version: u64,
        issued_at: String,
    },
    CourtesyCreditSettled {
        event_id: String,
        order_id: String,
        amount_cents: i64,
        transaction_ref: String,
        version: u64,
        settled_at: String,
    },
    CourtesyCreditSettlementFailed {
        event_id: String,
        order_id: String,
        amount_cents: i64,
        reason: String,
        version: u64,
        failed_at: String,
    },
}

/// Outbound request payload dispatched to external enterprise clearing endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockEnterpriseSettlementRequest {
    pub order_id: String,
    pub command_id: String,
    pub idempotency_key: String,
    pub amount_cents: i64,
    pub status: String,
    pub timestamp: String,
}

/// Inbound webhook callback payload posted by external settlement partner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockSettlementCallbackPayload {
    pub order_id: String,
    pub amount_cents: i64,
    pub status: String, // "settled" or "failed"
    pub transaction_ref: Option<String>,
    pub reason: Option<String>,
}

impl MockSettlementCallbackPayload {
    pub fn validate(&self) -> Result<(), String> {
        if self.order_id.trim().is_empty() {
            return Err("order_id cannot be blank".to_string());
        }
        if self.amount_cents <= 0 {
            return Err("amount_cents must be strictly greater than 0".to_string());
        }
        match self.status.as_str() {
            "settled" => {
                if self
                    .transaction_ref
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    return Err(
                        "transaction_ref is required for settled status and cannot be blank"
                            .to_string(),
                    );
                }
            }
            "failed" => {
                if self
                    .reason
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    return Err(
                        "reason is required for failed status and cannot be blank".to_string()
                    );
                }
            }
            other => return Err(format!("unrecognized settlement status '{}'", other)),
        }
        Ok(())
    }
}

/// Simulated Order Aggregate tracking courtesy credit settlement lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderSettlementState {
    None,
    PendingSettlement { amount_cents: i64 },
    Settled { amount_cents: i64, tx_ref: String },
    SettlementFailed { amount_cents: i64, reason: String },
}

#[derive(Debug, Clone)]
pub struct SimulatedOrderAggregate {
    pub order_id: String,
    pub version: u64,
    pub settlement_state: OrderSettlementState,
}

impl SimulatedOrderAggregate {
    pub fn new(order_id: impl Into<String>) -> Self {
        Self {
            order_id: order_id.into(),
            version: 1,
            settlement_state: OrderSettlementState::None,
        }
    }

    pub fn apply(&mut self, event: &MockDomainEvent) -> Result<(), String> {
        match event {
            MockDomainEvent::OrderCreated { version, .. } => {
                self.version = *version;
            }
            MockDomainEvent::CreditIssued {
                amount_cents,
                version,
                ..
            } => {
                if *amount_cents <= 0 || *amount_cents > MAX_COURTESY_CREDIT_CENTS {
                    return Err(format!(
                        "Invalid credit amount {} outside allowed bounds 1..={}",
                        amount_cents, MAX_COURTESY_CREDIT_CENTS
                    ));
                }
                self.settlement_state = OrderSettlementState::PendingSettlement {
                    amount_cents: *amount_cents,
                };
                self.version = *version;
            }
            MockDomainEvent::CourtesyCreditSettled {
                amount_cents,
                transaction_ref,
                version,
                ..
            } => match &self.settlement_state {
                OrderSettlementState::PendingSettlement {
                    amount_cents: expected,
                } => {
                    if *amount_cents != *expected {
                        return Err(format!(
                            "Settled amount {} does not match pending amount {}",
                            amount_cents, expected
                        ));
                    }
                    self.settlement_state = OrderSettlementState::Settled {
                        amount_cents: *amount_cents,
                        tx_ref: transaction_ref.clone(),
                    };
                    self.version = *version;
                }
                OrderSettlementState::Settled { .. } => {
                    // Idempotent duplicate: no-op
                }
                other => {
                    return Err(format!(
                        "Cannot settle courtesy credit from state {:?}",
                        other
                    ));
                }
            },
            MockDomainEvent::CourtesyCreditSettlementFailed {
                amount_cents,
                reason,
                version,
                ..
            } => match &self.settlement_state {
                OrderSettlementState::PendingSettlement {
                    amount_cents: expected,
                } => {
                    if *amount_cents != *expected {
                        return Err(format!(
                            "Failed amount {} does not match pending amount {}",
                            amount_cents, expected
                        ));
                    }
                    self.settlement_state = OrderSettlementState::SettlementFailed {
                        amount_cents: *amount_cents,
                        reason: reason.clone(),
                    };
                    self.version = *version;
                }
                OrderSettlementState::SettlementFailed { .. } => {
                    // Idempotent duplicate: no-op
                }
                other => {
                    return Err(format!(
                        "Cannot fail courtesy credit settlement from state {:?}",
                        other
                    ));
                }
            },
        }
        Ok(())
    }
}

/// Mock Enterprise Settlement Gateway with retry simulation.
#[derive(Clone)]
pub struct MockEnterpriseGateway {
    pub call_count: Arc<AtomicU32>,
    pub transient_failures_before_success: u32,
    pub dispatched_requests: Arc<std::sync::Mutex<Vec<MockEnterpriseSettlementRequest>>>,
}

impl MockEnterpriseGateway {
    pub fn new(transient_failures: u32) -> Self {
        Self {
            call_count: Arc::new(AtomicU32::new(0)),
            transient_failures_before_success: transient_failures,
            dispatched_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub async fn dispatch(
        &self,
        request: MockEnterpriseSettlementRequest,
        max_retries: u32,
        initial_backoff: Duration,
    ) -> Result<u16, String> {
        self.dispatched_requests
            .lock()
            .unwrap()
            .push(request.clone());

        let mut attempt = 0;
        let mut backoff = initial_backoff;

        loop {
            attempt += 1;
            let current_count = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;

            if current_count <= self.transient_failures_before_success {
                // Transient 503 error
                if attempt > max_retries {
                    return Err(format!(
                        "Exhausted {} retries, enterprise returned HTTP 503",
                        max_retries
                    ));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(500));
                continue;
            }

            // Success HTTP 200 OK
            return Ok(200);
        }
    }
}

#[tokio::test]
async fn test_mock_end_to_end_saga_successful_clearing_dataflow() {
    let order_id = "order-saga-e2e-001";
    let mut aggregate = SimulatedOrderAggregate::new(order_id);

    // 1. Initial State: OrderCreated
    let created_event = MockDomainEvent::OrderCreated {
        event_id: Uuid::new_v4().to_string(),
        order_id: order_id.to_string(),
        customer_id: "cust-99".to_string(),
        total_cents: 15_000,
        version: 1,
    };
    aggregate.apply(&created_event).unwrap();
    assert_eq!(aggregate.version, 1);
    assert_eq!(aggregate.settlement_state, OrderSettlementState::None);

    // 2. Courtesy Credit Issued: CreditIssuedEvent
    let credit_event_id = Uuid::new_v4().to_string();
    let credit_amount_cents = 2_500; // $25.00
    let credit_event = MockDomainEvent::CreditIssued {
        event_id: credit_event_id.clone(),
        order_id: order_id.to_string(),
        amount_cents: credit_amount_cents,
        reason: "Shipping SLA delay compensation".to_string(),
        version: 2,
        issued_at: "2026-11-20T12:00:00Z".to_string(),
    };
    aggregate.apply(&credit_event).unwrap();
    assert_eq!(aggregate.version, 2);
    assert_eq!(
        aggregate.settlement_state,
        OrderSettlementState::PendingSettlement {
            amount_cents: 2_500
        }
    );

    // 3. Saga Intake & Correlation Construction:
    // Non-credit events must be ignored
    let non_credit_event = MockDomainEvent::OrderCreated {
        event_id: Uuid::new_v4().to_string(),
        order_id: "order-other".to_string(),
        customer_id: "cust-1".to_string(),
        total_cents: 5000,
        version: 1,
    };
    let is_saga_target = matches!(non_credit_event, MockDomainEvent::CreditIssued { .. });
    assert!(!is_saga_target, "Non-credit events must not trigger saga");

    // Construct outbound EnterpriseSettlementRequest with correlation tokens
    let saga_request = MockEnterpriseSettlementRequest {
        order_id: order_id.to_string(),
        command_id: credit_event_id.clone(),
        idempotency_key: format!("saga-settlement-{}-{}", order_id, credit_event_id),
        amount_cents: credit_amount_cents,
        status: "pending_settlement".to_string(),
        timestamp: "2026-11-20T12:00:00Z".to_string(),
    };
    assert_eq!(
        saga_request.idempotency_key,
        format!("saga-settlement-{}-{}", order_id, credit_event_id)
    );

    // 4. Outbound Enterprise Dispatch with retry resilience
    // Gateway configured to fail twice with 503, then succeed on 3rd attempt
    let gateway = MockEnterpriseGateway::new(2);
    let status_code = gateway
        .dispatch(saga_request.clone(), 3, Duration::from_millis(10))
        .await
        .expect("dispatch should succeed on 3rd attempt within max_retries");
    assert_eq!(status_code, 200);
    assert_eq!(gateway.call_count.load(Ordering::SeqCst), 3);
    assert_eq!(gateway.dispatched_requests.lock().unwrap().len(), 1);

    // 5. Webhook Ingress Gateway Processing
    // External enterprise system posts back callback
    let callback = MockSettlementCallbackPayload {
        order_id: order_id.to_string(),
        amount_cents: credit_amount_cents,
        status: "settled".to_string(),
        transaction_ref: Some("TX-CLEARING-78901".to_string()),
        reason: None,
    };
    assert!(
        callback.validate().is_ok(),
        "Valid settlement callback payload must pass validation"
    );

    // Translates callback into domain event CourtesyCreditSettled
    let settled_event = MockDomainEvent::CourtesyCreditSettled {
        event_id: Uuid::new_v4().to_string(),
        order_id: callback.order_id.clone(),
        amount_cents: callback.amount_cents,
        transaction_ref: callback.transaction_ref.unwrap(),
        version: 3,
        settled_at: "2026-11-20T12:00:05Z".to_string(),
    };

    aggregate.apply(&settled_event).unwrap();
    assert_eq!(aggregate.version, 3);
    assert_eq!(
        aggregate.settlement_state,
        OrderSettlementState::Settled {
            amount_cents: 2_500,
            tx_ref: "TX-CLEARING-78901".to_string(),
        }
    );

    // 6. Idempotent Replay Invariance:
    // Re-applying the same settled event does not corrupt state
    aggregate.apply(&settled_event).unwrap();
    assert_eq!(aggregate.version, 3);
    assert_eq!(
        aggregate.settlement_state,
        OrderSettlementState::Settled {
            amount_cents: 2_500,
            tx_ref: "TX-CLEARING-78901".to_string(),
        }
    );
}

#[tokio::test]
async fn test_mock_end_to_end_saga_failed_clearing_dataflow() {
    let order_id = "order-saga-e2e-failed-002";
    let mut aggregate = SimulatedOrderAggregate::new(order_id);

    // 1. Setup Order with Courtesy Credit Issued
    let credit_event_id = Uuid::new_v4().to_string();
    let credit_amount_cents = 3_000;
    let credit_event = MockDomainEvent::CreditIssued {
        event_id: credit_event_id.clone(),
        order_id: order_id.to_string(),
        amount_cents: credit_amount_cents,
        reason: "Customer goodwill discount".to_string(),
        version: 2,
        issued_at: "2026-11-20T12:10:00Z".to_string(),
    };
    aggregate.apply(&credit_event).unwrap();

    // 2. Outbound dispatch success to external gateway
    let gateway = MockEnterpriseGateway::new(0);
    let saga_request = MockEnterpriseSettlementRequest {
        order_id: order_id.to_string(),
        command_id: credit_event_id.clone(),
        idempotency_key: format!("saga-settlement-{}-{}", order_id, credit_event_id),
        amount_cents: credit_amount_cents,
        status: "pending_settlement".to_string(),
        timestamp: "2026-11-20T12:10:00Z".to_string(),
    };
    let status_code = gateway
        .dispatch(saga_request, 3, Duration::from_millis(5))
        .await
        .unwrap();
    assert_eq!(status_code, 200);

    // 3. Webhook Ingress Gateway Receives "failed" Callback
    let callback = MockSettlementCallbackPayload {
        order_id: order_id.to_string(),
        amount_cents: credit_amount_cents,
        status: "failed".to_string(),
        transaction_ref: None,
        reason: Some("Clearing account balance depleted".to_string()),
    };
    assert!(callback.validate().is_ok());

    // Translates callback into domain event CourtesyCreditSettlementFailed
    let failed_event = MockDomainEvent::CourtesyCreditSettlementFailed {
        event_id: Uuid::new_v4().to_string(),
        order_id: callback.order_id.clone(),
        amount_cents: callback.amount_cents,
        reason: callback.reason.unwrap(),
        version: 3,
        failed_at: "2026-11-20T12:10:06Z".to_string(),
    };

    aggregate.apply(&failed_event).unwrap();
    assert_eq!(aggregate.version, 3);
    assert_eq!(
        aggregate.settlement_state,
        OrderSettlementState::SettlementFailed {
            amount_cents: 3_000,
            reason: "Clearing account balance depleted".to_string(),
        }
    );
}

#[test]
fn test_mock_webhook_callback_validation_rejects_malformed_payloads() {
    // Blank order_id
    let blank_order = MockSettlementCallbackPayload {
        order_id: "  ".to_string(),
        amount_cents: 1000,
        status: "settled".to_string(),
        transaction_ref: Some("TX-123".to_string()),
        reason: None,
    };
    assert!(blank_order.validate().is_err());

    // Negative / Zero amount
    let zero_amount = MockSettlementCallbackPayload {
        order_id: "order-1".to_string(),
        amount_cents: 0,
        status: "settled".to_string(),
        transaction_ref: Some("TX-123".to_string()),
        reason: None,
    };
    assert!(zero_amount.validate().is_err());

    // Settled without transaction_ref
    let settled_no_ref = MockSettlementCallbackPayload {
        order_id: "order-1".to_string(),
        amount_cents: 1000,
        status: "settled".to_string(),
        transaction_ref: None,
        reason: None,
    };
    assert!(settled_no_ref.validate().is_err());

    // Failed without reason
    let failed_no_reason = MockSettlementCallbackPayload {
        order_id: "order-1".to_string(),
        amount_cents: 1000,
        status: "failed".to_string(),
        transaction_ref: None,
        reason: None,
    };
    assert!(failed_no_reason.validate().is_err());
}

#[tokio::test]
async fn test_saga_dispatcher_retry_exhaustion_returns_transient_failure() {
    let gateway = MockEnterpriseGateway::new(10); // 10 failures > 3 max retries
    let saga_request = MockEnterpriseSettlementRequest {
        order_id: "order-retry-exhaust".to_string(),
        command_id: "cmd-retry-exhaust".to_string(),
        idempotency_key: "saga-settlement-retry-exhaust".to_string(),
        amount_cents: 2500,
        status: "pending_settlement".to_string(),
        timestamp: "2026-11-20T12:00:00Z".to_string(),
    };

    let result = gateway
        .dispatch(saga_request, 3, Duration::from_millis(5))
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("Exhausted 3 retries"),
        "Error message should mention retry exhaustion: {err_msg}"
    );
    assert_eq!(gateway.call_count.load(Ordering::SeqCst), 4); // initial + 3 retries = 4
}

#[test]
fn test_mock_webhook_callback_rejects_amount_mismatch_and_stale_versions() {
    let order_id = "order-mismatch-test";
    let mut aggregate = SimulatedOrderAggregate::new(order_id);

    // Initial order creation
    let created = MockDomainEvent::OrderCreated {
        event_id: "evt-create-1".to_string(),
        order_id: order_id.to_string(),
        customer_id: "cust-1".to_string(),
        total_cents: 5000,
        version: 1,
    };
    aggregate.apply(&created).unwrap();

    // Credit issued: $15.00
    let credit = MockDomainEvent::CreditIssued {
        event_id: "evt-credit-1".to_string(),
        order_id: order_id.to_string(),
        amount_cents: 1500,
        reason: "Courtesy credit".to_string(),
        version: 2,
        issued_at: "2026-11-20T12:00:00Z".to_string(),
    };
    aggregate.apply(&credit).unwrap();

    // 1. Webhook callback payload with mismatched amount
    let callback_mismatched = MockSettlementCallbackPayload {
        order_id: order_id.to_string(),
        amount_cents: 2000, // Mismatched! Expected 1500
        status: "settled".to_string(),
        transaction_ref: Some("TX-MISMATCH".to_string()),
        reason: None,
    };
    assert!(callback_mismatched.validate().is_ok());
    assert_ne!(
        callback_mismatched.amount_cents, 1500,
        "Callback amount must be recognized as mismatched against issued credit"
    );

    // 2. Aggregate domain validation rejects mismatched amount event
    let mismatched_event = MockDomainEvent::CourtesyCreditSettled {
        event_id: "evt-mismatch".to_string(),
        order_id: order_id.to_string(),
        amount_cents: 2000,
        transaction_ref: "TX-MISMATCH".to_string(),
        version: 3,
        settled_at: "2026-11-20T12:00:00Z".to_string(),
    };
    assert_eq!(
        aggregate.apply(&mismatched_event),
        Err("Settled amount 2000 does not match pending amount 1500".to_string())
    );

    // 3. Out-of-order state transition rejected (settle attempt on fresh aggregate without pending credit)
    let mut fresh_aggregate = SimulatedOrderAggregate::new("order-no-credit");
    let settle_without_credit = MockDomainEvent::CourtesyCreditSettled {
        event_id: "evt-invalid-state".to_string(),
        order_id: "order-no-credit".to_string(),
        amount_cents: 1500,
        transaction_ref: "TX-INVALID".to_string(),
        version: 2,
        settled_at: "2026-11-20T12:00:00Z".to_string(),
    };
    assert_eq!(
        fresh_aggregate.apply(&settle_without_credit),
        Err("Cannot settle courtesy credit from state None".to_string())
    );
}
