//! Unified Cross-Platform Streaming Administration CLI (Story 13.10, Story 16.14)
//! Coordinates Apache Iggy provisioning, consumer groups, broker verification, and Delta storage initialization.

use clap::{Parser, Subcommand};
use ferrous_cx::admin::{
    run_all, run_init_delta_storage, run_load_olist_dimensions, run_provision_consumer_groups,
    run_provision_topics, run_validate_connectors, run_verify_broker, DEFAULT_IGGY_URL,
};
use ferrous_cx::tidb::{
    connect_and_init_tidb, load_olist_dimensions_to_tidb, DEFAULT_TIDB_URL,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "streaming-admin",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Cross-Platform Streaming Administration Suite for Apache Iggy & Delta Lake"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Provision stream 'orders' and partitioned topics 'intake' and 'consolidated' (Step 2)
    ProvisionTopics {
        /// Apache Iggy HTTP URL
        #[arg(short, long, visible_alias = "iggy-url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without mutating live broker
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Provision zero-copy consumer groups cg-arroyo-raw, cg-cmd-handler, cg-arroyo-state (Step 3)
    ProvisionConsumerGroups {
        /// Apache Iggy HTTP URL
        #[arg(short, long, visible_alias = "iggy-url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without mutating live broker
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Verify Apache Iggy broker health and consumer group attachments (Step 4)
    VerifyBroker {
        /// Apache Iggy HTTP URL
        #[arg(long, visible_alias = "url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        iggy_url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Run in dry-run conformance mode without network probes
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Initialize Delta Lake sink storage directories and schemas (Step 5)
    InitDeltaStorage {
        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Run in dry-run conformance mode without mutating disk
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Validate Apache Iggy delta_sink connector configuration and target tables (Step 6)
    ValidateConnectors {
        /// Path to connector configuration file
        #[arg(short, long, default_value = "config/streaming/connectors/delta_sink.toml")]
        config: PathBuf,

        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Run in dry-run conformance mode without disk checks
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Batch-load the 5 static Kaggle Olist dimension CSVs into Delta Lake (Story 22.2, AD-35)
    LoadOlistDimensions {
        /// Directory containing Kaggle Olist CSV files
        #[arg(long, default_value = "docs/datasets/olist")]
        input_dir: PathBuf,

        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Run in dry-run mode without mutating disk
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Initialize canonical 9-table Olist + CQRS TiDB SQL schema (Story 23.1, AD-36)
    InitTidbSchema {
        /// TiDB MySQL-protocol connection URL
        #[arg(long, env = "TIDB_URL", default_value = DEFAULT_TIDB_URL)]
        tidb_url: String,

        /// Run in dry-run mode without connecting to TiDB
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Batch-UPSERT the 5 static Kaggle Olist dimension CSVs directly into TiDB (Story 23.1, AD-36)
    LoadOlistTidb {
        /// Directory containing Kaggle Olist CSV files
        #[arg(long, default_value = "docs/datasets/olist")]
        input_dir: PathBuf,

        /// TiDB MySQL-protocol connection URL
        #[arg(long, env = "TIDB_URL", default_value = DEFAULT_TIDB_URL)]
        tidb_url: String,

        /// Run in dry-run mode without connecting to TiDB
        #[arg(short, long)]
        dry_run: bool,
    },

    /// Execute the full streaming setup sequence in order (Steps 2 -> 3 -> 4 -> 5 -> 6)
    All {
        /// Apache Iggy HTTP URL
        #[arg(long, visible_alias = "url", env = "IGGY_HTTP_URL", default_value = DEFAULT_IGGY_URL)]
        iggy_url: String,

        /// Iggy management username
        #[arg(long, env = "IGGY_ADMIN_USERNAME", default_value = "iggy")]
        username: String,

        /// Iggy management password
        #[arg(long, env = "IGGY_ADMIN_PASSWORD", default_value = "iggy")]
        password: String,

        /// Path to base Delta Lake storage directory
        #[arg(long, default_value = "data/delta")]
        delta_dir: PathBuf,

        /// Optional path to connector configuration file (delta_sink.toml)
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Run in dry-run conformance mode
        #[arg(short, long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Command::ProvisionTopics {
            url,
            username,
            password,
            dry_run,
        } => {
            run_provision_topics(&url, &username, &password, dry_run).await?;
        }
        Command::ProvisionConsumerGroups {
            url,
            username,
            password,
            dry_run,
        } => {
            run_provision_consumer_groups(&url, &username, &password, dry_run).await?;
        }
        Command::VerifyBroker {
            iggy_url,
            username,
            password,
            dry_run,
        } => {
            run_verify_broker(&iggy_url, &username, &password, dry_run).await?;
        }
        Command::InitDeltaStorage {
            delta_dir,
            dry_run,
        } => {
            run_init_delta_storage(&delta_dir, dry_run).await?;
        }
        Command::ValidateConnectors {
            config,
            delta_dir,
            dry_run,
        } => {
            run_validate_connectors(&config, Some(&delta_dir), dry_run).await?;
        }
        Command::LoadOlistDimensions {
            input_dir,
            delta_dir,
            dry_run,
        } => {
            run_load_olist_dimensions(&input_dir, &delta_dir, dry_run).await?;
        }
        Command::InitTidbSchema { tidb_url, dry_run } => {
            if dry_run {
                println!("[DRY-RUN] Would apply canonical 9-table Olist + CQRS DDL to {tidb_url}");
            } else {
                let _pool = connect_and_init_tidb(&tidb_url, 4).await?;
                println!("[init-tidb-schema] Applied canonical 9-table Olist + CQRS schema to TiDB");
            }
        }
        Command::LoadOlistTidb {
            input_dir,
            tidb_url,
            dry_run,
        } => {
            if dry_run {
                println!(
                    "[DRY-RUN] Would batch-UPSERT 5 Olist dimension CSVs from {} into {tidb_url}",
                    input_dir.display()
                );
            } else {
                let pool = connect_and_init_tidb(&tidb_url, 4).await?;
                let loaded = load_olist_dimensions_to_tidb(&pool, &input_dir).await?;
                for (tbl, cnt) in loaded {
                    println!("[load-olist-tidb] Upserted {cnt} rows into {tbl}");
                }
            }
        }
        Command::All {
            iggy_url,
            username,
            password,
            delta_dir,
            config,
            dry_run,
        } => {
            run_all(
                &iggy_url,
                &username,
                &password,
                &delta_dir,
                config.as_deref(),
                dry_run,
            )
            .await?;
        }
    }

    Ok(())
}
