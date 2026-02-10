use std::time::Duration;

use getitdone::Config;
use qrt_log_utils::{init_logger, LoggerConfig, LoggerContext};

pub const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";
pub const DEFAULT_MONGO_URI: &str = "mongodb://localhost:27017";
pub const DEFAULT_DB: &str = "getitdone_mock_e2e";

pub fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

pub fn build_config(collection: &str) -> Config {
    let mongo_uri = env("GETITDONE_MONGO_URI", DEFAULT_MONGO_URI);
    let database = env("GETITDONE_DB", DEFAULT_DB);
    let collection = env("GETITDONE_COLLECTION", collection);
    Config::builder()
        .mongo_uri(mongo_uri)
        .database(database)
        .collection(collection)
        .request_timeout(Some(Duration::from_secs(30)))
        .worker_switch_timeout(Duration::from_secs(10))
        // For repeated runs with same task_id during debugging.
        .reset_finished_tasks(true)
        .build()
}

pub fn init_tracing(service_name: &'static str) -> LoggerContext {
    let endpoint = env("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT);

    let builder = LoggerConfig::builder()
        .endpoint(endpoint)
        .add_blacklist_crates(["hyper", "tonic", "h2", "reqwest"]);

    let logger_config = builder.build();
    init_logger(service_name, logger_config)
}
