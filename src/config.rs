use std::time::Duration;

/// Configuration shared by callers and workers.
#[derive(Clone, Debug)]
pub struct Config {
    pub mongo_uri: String,
    pub database: String,
    pub collection: String,
    pub request_timeout: Option<Duration>,
    pub worker_switch_timeout: Duration,
    pub allow_reset_finished_tasks: bool,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

#[derive(Default, Debug)]
pub struct ConfigBuilder {
    mongo_uri: Option<String>,
    database: Option<String>,
    collection: Option<String>,
    request_timeout: Option<Option<Duration>>,
    worker_switch_timeout: Option<Duration>,
    reset_finished_to_pending: bool,
}

impl ConfigBuilder {
    pub fn mongo_uri(mut self, uri: impl Into<String>) -> Self {
        self.mongo_uri = Some(uri.into());
        self
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    pub fn collection(mut self, name: impl Into<String>) -> Self {
        self.collection = Some(name.into());
        self
    }

    /// Defaults to `None`, meaning the caller waits indefinitely.
    pub fn request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Defaults to 10 seconds.
    pub fn worker_switch_timeout(mut self, timeout: Duration) -> Self {
        self.worker_switch_timeout = Some(timeout);
        self
    }

    /// Allow callers to reuse task ids even if documents are already succeeded/failed.
    pub fn reset_finished_tasks(mut self, enable: bool) -> Self {
        self.reset_finished_to_pending = enable;
        self
    }

    pub fn build(self) -> Config {
        self.finalize()
    }

    fn finalize(self) -> Config {
        let mongo_uri = self.mongo_uri.expect("Config::mongo_uri must be provided");
        Config {
            mongo_uri,
            database: self.database.unwrap_or_else(|| "getitdone".into()),
            collection: self.collection.unwrap_or_else(|| "tasks".into()),
            request_timeout: self.request_timeout.unwrap_or(None),
            worker_switch_timeout: self
                .worker_switch_timeout
                .unwrap_or_else(|| Duration::from_secs(10)),
            allow_reset_finished_tasks: self.reset_finished_to_pending,
        }
    }
}
