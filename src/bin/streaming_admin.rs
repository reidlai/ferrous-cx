//! Unified Cross-Platform Streaming Administration CLI (Story 13.10, Story 16.14)
//! Coordinates Apache Iggy provisioning, consumer groups, broker verification, and Delta storage initialization.

use clap::{Parser, Subcommand};
use ferrous_cx::admin::{
    run_all, run_init_delta_storage, run_provision_consumer_groups, run_provision_topics,
    run_verify_broker, DEFAULT_IGGY_URL,
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

    /// Execute the full streaming setup sequence in order (Steps 2 -> 3 -> 4 -> 5)
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
        Command::All {
            iggy_url,
            username,
            password,
            delta_dir,
            dry_run,
        } => {
            run_all(&iggy_url, &username, &password, &delta_dir, dry_run).await?;
        }
    }

    Ok(())
}
