use crate::progress::{set_min_refresh_delay, set_quiet_output};
use clap::{Args, Parser, Subcommand, ValueEnum};
use fred::types::RedisValue;

#[derive(Parser, Debug, Clone, Default)]
#[command(
  version,
  about,
  long_about = "Utilities for inspecting a Redis keyspace via the SCAN command."
)]
#[command(propagate_version = true)]
pub struct Argv {
  // Shared Arguments
  /// The server hostname.
  #[arg(short = 'H', long = "host", default_value = "127.0.0.1", value_name = "STRING")]
  pub host:             String,
  /// The server port.
  #[arg(short = 'p', long = "port", default_value = "6379", value_name = "NUMBER")]
  pub port:             u16,
  /// The database to `SELECT` after connecting.
  #[arg(long = "db", value_name = "NUMBER")]
  pub db:               Option<u8>,
  /// The username to provide when authenticating.
  #[arg(short = 'u', long = "username", env = "REDIS_USERNAME", value_name = "STRING")]
  pub username:         Option<String>,
  /// The password to provide after connection.
  #[arg(short = 'P', long = "password", env = "REDIS_PASSWORD", value_name = "STRING")]
  pub password:         Option<String>,
  /// The name of the sentinel service, if using a sentinel deployment.
  #[arg(long = "sentinel-service", value_name = "STRING", conflicts_with = "cluster")]
  pub sentinel_service: Option<String>,
  /// Whether to discover other nodes in a Redis cluster.
  #[arg(short = 'c', long = "cluster", default_value = "false")]
  pub cluster:          bool,
  /// Whether to scan replicas rather than primary nodes. This also implies `--cluster`.
  #[arg(short = 'r', long = "replicas", default_value = "false")]
  pub replicas:         bool,
  /// Whether to hide progress bars and messages before the final output.
  #[arg(short = 'q', long = "quiet", default_value = "false")]
  pub quiet:            bool,
  /// Ignore errors, if possible.
  #[arg(short = 'i', long = "ignore", default_value = "false")]
  pub ignore:           bool,
  /// An optional reconnection delay. If not provided the client will stop scanning after any disconnection.
  #[arg(short = 'R', long = "reconnect", value_name = "NUMBER")]
  pub reconnect:        Option<u32>,

  // TLS Arguments
  /// Whether to use TLS when connecting to servers.
  #[arg(long = "tls", default_value = "false")]
  pub tls:         bool,
  /// A file path to the private key for a x509 identity used by the client.
  #[arg(long = "tls-key", value_name = "PATH")]
  pub tls_key:     Option<String>,
  /// A file path to the certificate for a x509 identity used by the client.
  #[arg(long = "tls-cert", value_name = "PATH")]
  pub tls_cert:    Option<String>,
  /// A file path to a trusted certificate bundle.
  #[arg(long = "tls-ca-cert", value_name = "PATH")]
  pub tls_ca_cert: Option<String>,

  // Shared Scan Arguments
  /// The glob pattern to provide in each `SCAN` command.
  #[arg(long = "pattern", value_name = "STRING", default_value = "*")]
  pub pattern:   String,
  /// The number of results to request in each `SCAN` command.
  #[arg(long = "page-size", default_value = "100", allow_negative_numbers = false)]
  pub page_size: u32,
  /// A delay, in milliseconds, to wait between `SCAN` commands.
  #[arg(short = 'd', long = "delay", default_value = "0")]
  pub delay:     u64,
  /// A regular expression used to filter keys while scanning. Keys that do not match will be skipped before
  /// any subsequent operations are performed.
  #[arg(short = 'f', long = "filter", value_name = "REGEX")]
  pub filter:    Option<String>,
  /// A regular expression used to reject or skip keys while scanning. Keys that match will be skipped before
  /// any subsequent operations are performed.
  #[arg(short = 's', long = "reject", value_name = "REGEX")]
  pub reject:    Option<String>,

  /// Set a minimum refresh delay between progress bar updates, in milliseconds.
  #[arg(long = "min-refresh-delay", value_name = "NUMBER")]
  pub refresh: Option<u64>,
  // Command Arguments
  #[command(subcommand)]
  pub command: Commands,
}

impl Argv {
  pub fn set_globals(&self) {
    if let Some(dur) = self.refresh {
      set_min_refresh_delay(dur as usize);
    }
    if self.quiet {
      set_quiet_output(true);
    }
  }

  // there's gotta be a better way to do this
  pub fn fix(mut self) -> Self {
    if self.tls_cert.is_some() || self.tls_key.is_some() || self.tls_ca_cert.is_some() {
      self.tls = true;
    }

    if self.replicas {
      self.cluster = true;
    }
    // env vars can be Some("") when left unset
    if let Some(username) = self.username.take() {
      if !username.is_empty() {
        self.username = Some(username);
      }
    }
    if let Some(password) = self.password.take() {
      if !password.is_empty() {
        self.password = Some(password);
      }
    }

    self
  }

  pub fn output_file(&self) -> Option<String> {
    match self.command {
      Commands::Memory(ref argv) => argv.file.clone(),
      Commands::Idle(ref argv) => argv.file.clone(),
      Commands::Ttl(ref argv) => argv.file.clone(),
      _ => None,
    }
  }
}

#[derive(Subcommand, Clone, Debug)]
pub enum Commands {
  /// Inspect keys via the `OBJECT IDLETIME` command.
  Idle(IdleArgv),
  /// Inspect keys via the `MEMORY USAGE` command.
  Memory(MemoryArgv),
  /// Call `TOUCH` on each key.
  ///
  /// Default.
  Touch(TouchArgv),
  /// Inspect keys via the `TTL` command.
  Ttl(TtlArgv),
  /// Set an expiration on keys.
  Expire(ExpireArgv),
  /// Inspect values of keys via the `GET` command. Only for numerical values.
  Sum(SumArgv),
}

impl Default for Commands {
  fn default() -> Self {
    Commands::Touch(TouchArgv {})
  }
}

/// The available output formats.
#[derive(ValueEnum, Clone, Debug)]
pub enum OutputFormat {
  Table,
  Csv,
  Json,
}

impl Default for OutputFormat {
  fn default() -> Self {
    OutputFormat::Table
  }
}

/// The sort order to use.
#[derive(ValueEnum, Clone, Debug)]
pub enum Sort {
  Asc,
  Desc,
}

impl Default for Sort {
  fn default() -> Self {
    Sort::Desc
  }
}

/// An optional argument to `EXPIRE` (requires Redis >= 7.0.0).
#[derive(ValueEnum, Clone, Debug)]
pub enum ExpireArg {
  NX,
  XX,
  GT,
  LT,
}

impl ExpireArg {
  pub fn to_arg(&self) -> RedisValue {
    RedisValue::from_static_str(match self {
      ExpireArg::GT => "GT",
      ExpireArg::LT => "LT",
      ExpireArg::XX => "XX",
      ExpireArg::NX => "NX",
    })
  }
}

#[derive(Args, Clone, Debug, Default)]
pub struct IdleArgv {
  /// The output format, if applicable.
  #[arg(short = 'f', long = "format", default_value = "table", value_name = "STRING")]
  pub format: OutputFormat,
  /// The sort order to use.
  #[arg(short = 'S', long = "sort", default_value = "desc", value_name = "STRING")]
  pub sort:   Sort,
  /// The maximum number of results to return.
  #[arg(
    short = 'l',
    long = "limit",
    default_value = "100",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub limit:  u64,
  /// The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in
  /// memory.
  #[arg(
    short = 'o',
    long = "offset",
    default_value = "0",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub offset: u64,
  /// Write the final output to the provided file.
  #[arg(short = 'F', long = "file", value_name = "PATH")]
  pub file:   Option<String>,
}

#[derive(Args, Clone, Debug, Default)]
pub struct MemoryArgv {
  /// The output format, if applicable.
  #[arg(short = 'f', long = "format", default_value = "table", value_name = "STRING")]
  pub format:                OutputFormat,
  /// The sort order to use.
  #[arg(short = 'S', long = "sort", default_value = "desc", value_name = "STRING")]
  pub sort:                  Sort,
  /// A regular expression used to group or transform keys (via `Regex::captures`) while aggregating results. This is
  /// often used to extract substrings in a key.
  #[arg(short = 'g', long = "group-by", value_name = "REGEX")]
  pub group_by:              Option<String>,
  /// A delimiter used to `slice::join` multiple values from `--group-by`, if applicable.
  #[arg(long = "group-by-delimiter", value_name = "STRING", default_value = ":")]
  pub group_by_delimiter:    String,
  /// Whether to skip keys that do not capture anything from the `--group-by` regex.
  #[arg(long = "filter-missing-groups", default_value = "false")]
  pub filter_missing_groups: bool,
  /// The maximum number of results to return.
  #[arg(
    short = 'l',
    long = "limit",
    default_value = "100",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub limit:                 u64,
  /// The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in
  /// memory.
  #[arg(
    short = 'o',
    long = "offset",
    default_value = "0",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub offset:                u64,
  /// The number of records to index in memory while scanning. Default is `--limit + --offset`.
  #[arg(long = "max-index-size", allow_negative_numbers = false, value_name = "NUMBER")]
  pub max_index_size:        Option<u64>,
  /// The number of samples to provide in each `MEMORY USAGE` command.
  #[arg(short = 's', long = "samples", allow_negative_numbers = false, value_name = "NUMBER")]
  pub samples:               Option<u32>,
  /// Write the final output to the provided file.
  #[arg(short = 'F', long = "file", value_name = "PATH")]
  pub file:                  Option<String>,
}

#[derive(Args, Clone, Debug, Default)]
pub struct SumArgv {
  /// The output format, if applicable.
  #[arg(short = 'f', long = "format", default_value = "table", value_name = "STRING")]
  pub format:                OutputFormat,
  /// The sort order to use.
  #[arg(short = 'S', long = "sort", default_value = "desc", value_name = "STRING")]
  pub sort:                  Sort,
  /// A regular expression used to group or transform keys (via `Regex::captures`) while aggregating results. This is
  /// often used to extract substrings in a key.
  #[arg(short = 'g', long = "group-by", value_name = "REGEX")]
  pub group_by:              Option<String>,
  /// A delimiter used to `slice::join` multiple values from `--group-by`, if applicable.
  #[arg(long = "group-by-delimiter", value_name = "STRING", default_value = ":")]
  pub group_by_delimiter:    String,
  /// Whether to skip keys that do not capture anything from the `--group-by` regex.
  #[arg(long = "filter-missing-groups", default_value = "false")]
  pub filter_missing_groups: bool,
  /// The maximum number of results to return.
  #[arg(
    short = 'l',
    long = "limit",
    default_value = "100",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub limit:                 u64,
  /// The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in
  /// memory.
  #[arg(
    short = 'o',
    long = "offset",
    default_value = "0",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub offset:                u64,
  /// The number of records to index in memory while scanning. Default is `--limit + --offset`.
  #[arg(long = "max-index-size", allow_negative_numbers = false, value_name = "NUMBER")]
  pub max_index_size:        Option<u64>,
  /// Write the final output to the provided file.
  #[arg(short = 'F', long = "file", value_name = "PATH")]
  pub file:                  Option<String>,
}

#[derive(Args, Clone, Debug, Default)]
pub struct TouchArgv {}

#[derive(Args, Clone, Debug, Default)]
pub struct TtlArgv {
  /// The output format, if applicable.
  #[arg(short = 'f', long = "format", default_value = "table", value_name = "STRING")]
  pub format:       OutputFormat,
  /// The sort order to use.
  #[arg(short = 'S', long = "sort", default_value = "desc", value_name = "STRING")]
  pub sort:         Sort,
  /// The maximum number of results to return.
  #[arg(
    short = 'l',
    long = "limit",
    default_value = "100",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub limit:        u64,
  /// The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in
  /// memory.
  #[arg(
    short = 'o',
    long = "offset",
    default_value = "0",
    allow_negative_numbers = false,
    value_name = "NUMBER"
  )]
  pub offset:       u64,
  /// Skip keys that do not have a TTL. Missing TTLs act as `-1` for sorting purposes.
  #[arg(long = "skip-missing", default_value = "false")]
  pub skip_missing: bool,
  /// Write the final output to the provided file.
  #[arg(short = 'F', long = "file", value_name = "PATH")]
  pub file:         Option<String>,
}

#[derive(Args, Clone, Debug, Default)]
pub struct ExpireArgv {
  /// The number of seconds provided to `EXPIRE`.
  #[arg(short = 's', long = "seconds", required = true, value_name = "NUMBER")]
  pub seconds:   i64,
  /// An optional qualifier for the `EXPIRE` command (requires Redis >= 7.0.0).
  #[arg(short = 'q', long = "qualifier")]
  pub qualifier: Option<ExpireArg>,
  /// Send the `EXPIRE` command instead of performing a dry run.
  #[arg(long = "really")]
  pub really:    bool,
}
