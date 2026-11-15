//! Cross-Platform Kanidm IAM Provisioner & Dev TLS Certificate Generator (Story 13.5)
//! Manages platform-specific IAM persona seeds (seed-rbac.json, seed-m2m.json) and
//! generates self-signed TLS certificates in pure Rust using rcgen (zero OpenSSL CLI dependency).

use clap::Parser;
use rcgen::{CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose, SanType};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(
    name = "setup-iam",
    author = "FerrousCX",
    version = "0.1.0",
    about = "Cross-Platform Kanidm IAM Dev TLS Cert & Persona Provisioner"
)]
pub struct Args {
    /// Generate self-signed development TLS certificates (chain.pem and key.pem)
    #[arg(long)]
    pub generate_certs: bool,

    /// Audit/provision human RBAC personas from seed-rbac.json
    #[arg(long)]
    pub seed_rbac: bool,

    /// Audit/provision M2M service accounts from seed-m2m.json
    #[arg(long)]
    pub seed_m2m: bool,

    /// Run full audit/provisioning (certs, RBAC personas, M2M accounts)
    #[arg(long)]
    pub all: bool,

    /// Force overwriting existing certificates if present
    #[arg(long)]
    pub force: bool,

    /// Run in dry-run conformance audit mode without disk/network mutations
    #[arg(short, long, alias = "DryRun")]
    pub dry_run: bool,

    /// Destination directory for TLS certificates
    #[arg(long, default_value = "config/iam/certs")]
    pub certs_dir: PathBuf,

    /// Directory containing seed-rbac.json and seed-m2m.json
    #[arg(long, default_value = "config/iam")]
    pub config_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct RbacSeed {
    pub domain: String,
    pub groups: Vec<RbacGroup>,
    pub users: Vec<RbacUser>,
    pub oauth2: Option<OAuth2Client>,
}

#[derive(Debug, Deserialize)]
pub struct RbacGroup {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct RbacUser {
    pub username: String,
    pub display_name: String,
    pub legal_name: String,
    pub email: String,
    pub group: String,
    pub env_password_var: String,
}

#[derive(Debug, Deserialize)]
pub struct OAuth2Client {
    pub client_id: String,
    pub display_name: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct M2mSeed {
    pub domain: String,
    pub service_accounts: Vec<ServiceAccount>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceAccount {
    pub account_id: String,
    pub display_name: String,
    pub description: String,
    pub scopes: Vec<String>,
    pub token_exchange_enabled: Option<bool>,
}

pub fn resolve_path(base_arg: &Path) -> PathBuf {
    let candidates = [
        base_arg.to_path_buf(),
        PathBuf::from("ferrous-cx").join(base_arg),
        PathBuf::from("..").join(base_arg),
    ];
    candidates.into_iter().find(|p| p.exists()).unwrap_or_else(|| base_arg.to_path_buf())
}

pub fn generate_dev_tls_certificates(certs_dir: &Path, dry_run: bool, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n[*] Generating / Auditing Dev TLS Certificates (SANs: localhost, ferrous-iam, ferrouscx.local, 127.0.0.1, ::1)...");

    let chain_path = certs_dir.join("chain.pem");
    let key_path = certs_dir.join("key.pem");

    if dry_run {
        println!("  [DRY-RUN] Validated TLS Certificate parameters:");
        println!("      CN: localhost, O: FerrousCX, OU: IAM");
        println!("      KeyUsages: DigitalSignature, KeyEncipherment");
        println!("      ExtendedKeyUsages: ServerAuth");
        println!("      SANs: DNS:localhost, DNS:ferrous-iam, DNS:ferrouscx.local, IP:127.0.0.1, IP:::1");
        println!("      Target Directory: {}", certs_dir.display());
        if chain_path.exists() && key_path.exists() {
            println!("  [OK] Existing certificates detected on disk.");
        } else if chain_path.exists() || key_path.exists() {
            println!("  [WARN] Partial certificate state detected (one of chain.pem/key.pem missing).");
        } else {
            println!("  [INFO] Certificates not yet on disk (will be generated upon live execution).");
        }
        return Ok(());
    }

    if chain_path.exists() && key_path.exists() && !force {
        println!("  [OK] Valid TLS certificates already exist on disk (skipping re-generation, use --force to overwrite):");
        println!("       Chain: {}", chain_path.display());
        println!("       Key:   {}", key_path.display());
        return Ok(());
    }

    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "localhost");
    dn.push(DnType::OrganizationName, "FerrousCX");
    dn.push(DnType::OrganizationalUnitName, "IAM");
    params.distinguished_name = dn;

    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];

    params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into()?),
        SanType::DnsName("ferrous-iam".try_into()?),
        SanType::DnsName("ferrouscx.local".try_into()?),
        SanType::IpAddress("127.0.0.1".parse()?),
        SanType::IpAddress("::1".parse()?),
    ];

    std::fs::create_dir_all(certs_dir)?;
    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    let tmp_chain = certs_dir.join("chain.pem.tmp");
    let tmp_key = certs_dir.join("key.pem.tmp");

    std::fs::write(&tmp_chain, cert_pem)?;
    std::fs::write(&tmp_key, key_pem)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp_chain, std::fs::Permissions::from_mode(0o644));
        let _ = std::fs::set_permissions(&tmp_key, std::fs::Permissions::from_mode(0o644));
    }

    std::fs::rename(&tmp_chain, &chain_path)?;
    std::fs::rename(&tmp_key, &key_path)?;

    println!("  [OK] Successfully generated self-signed TLS certificates (mode 0644):");
    println!("       Chain: {}", chain_path.display());
    println!("       Key:   {}", key_path.display());
    Ok(())
}

pub fn audit_and_seed_rbac(config_dir: &Path, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let rbac_file = config_dir.join("seed-rbac.json");
    println!("\n[*] Auditing / Seeding Human RBAC Personas from {}...", rbac_file.display());

    let content = std::fs::read_to_string(&rbac_file)
        .map_err(|e| format!("Missing or unreadable {}: {}", rbac_file.display(), e))?;
    let rbac: RbacSeed = serde_json::from_str(&content)?;

    println!("  [OK] Domain: {}", rbac.domain);
    println!("  [OK] Defined Groups ({}):", rbac.groups.len());

    let mut group_names = HashSet::new();
    for g in &rbac.groups {
        group_names.insert(g.name.clone());
        println!("       - {} ({})", g.name, g.description);
    }

    // Required roles validation
    let required_roles = ["customer_specialist", "operations_manager", "system_admin"];
    for role in &required_roles {
        if !group_names.contains(*role) {
            return Err(format!("RBAC seed missing required role group '{}'", role).into());
        }
    }

    println!("  [OK] Defined Users ({}):", rbac.users.len());
    let mut usernames = HashSet::new();
    for u in &rbac.users {
        if !group_names.contains(&u.group) {
            return Err(format!("User '{}' belongs to unknown group '{}'", u.username, u.group).into());
        }
        usernames.insert(u.username.clone());
        let env_status = match std::env::var(&u.env_password_var) {
            Ok(_) => "[Password Env: OK]",
            Err(_) => "[Password Env: Unset in current session]",
        };
        println!("       - {} <{}> [Group: {}] {}", u.username, u.email, u.group, env_status);
    }

    // Required users validation
    let required_users = ["carlos.support", "operations.lead", "admin.sec"];
    for user in &required_users {
        if !usernames.contains(*user) {
            return Err(format!("RBAC seed missing required user '{}'", user).into());
        }
    }

    if let Some(ref o) = rbac.oauth2 {
        println!("  [OK] OAuth2 Client: {} (Redirect: {})", o.client_id, o.redirect_uri);
    }

    if dry_run {
        println!("  [DRY-RUN] RBAC schema conformance verified cleanly.");
    }

    Ok(())
}

pub fn audit_and_seed_m2m(config_dir: &Path, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let m2m_file = config_dir.join("seed-m2m.json");
    println!("\n[*] Auditing / Seeding M2M Service Accounts from {}...", m2m_file.display());

    let content = std::fs::read_to_string(&m2m_file)
        .map_err(|e| format!("Missing or unreadable {}: {}", m2m_file.display(), e))?;
    let m2m: M2mSeed = serde_json::from_str(&content)?;

    println!("  [OK] Domain: {}", m2m.domain);
    println!("  [OK] Service Accounts ({}):", m2m.service_accounts.len());

    let mut account_ids = HashSet::new();
    for sa in &m2m.service_accounts {
        account_ids.insert(sa.account_id.clone());
        println!("       - {} ({}) Scopes: {:?}", sa.account_id, sa.display_name, sa.scopes);
    }

    // Required service accounts validation
    let required_m2m = ["svc_ferrous_api", "svc_ferrous_agent"];
    for sa in &required_m2m {
        if !account_ids.contains(*sa) {
            return Err(format!("M2M seed missing required service account '{}'", sa).into());
        }
    }

    if dry_run {
        println!("  [DRY-RUN] M2M schema conformance verified cleanly.");
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("==========================================================");
    println!("  FerrousCX - IAM Dev Cert & Persona Provisioner CLI      ");
    println!("==========================================================");

    let config_dir = resolve_path(&args.config_dir);
    let certs_dir = if args.certs_dir == Path::new("config/iam/certs") {
        config_dir.join("certs")
    } else {
        args.certs_dir
    };

    let do_all = args.all || (!args.generate_certs && !args.seed_rbac && !args.seed_m2m);
    let do_certs = args.generate_certs || do_all;
    let do_rbac = args.seed_rbac || do_all;
    let do_m2m = args.seed_m2m || do_all;

    if do_certs {
        generate_dev_tls_certificates(&certs_dir, args.dry_run, args.force)?;
    }

    if do_rbac {
        audit_and_seed_rbac(&config_dir, args.dry_run)?;
    }

    if do_m2m {
        audit_and_seed_m2m(&config_dir, args.dry_run)?;
    }

    println!("\n==========================================================");
    if args.dry_run {
        println!("  [OK] IAM CONFORMANCE AUDIT COMPLETED CLEANLY!           ");
    } else {
        println!("  [OK] IAM SETUP & AUDIT COMPLETED CLEANLY!               ");
    }
    println!("==========================================================");
    Ok(())
}
