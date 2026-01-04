use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("duplicate idempotency key for task {task_id}")]
    Duplicate { task_id: String },
    #[error("caller timed out waiting for response")]
    Timeout,
    #[error("worker crashed or disconnected before responding")]
    WorkerGone,
    #[error("no worker picked up the task before timeout")]
    WorkerTimeout,
    #[error("payload format mismatch in {field}")]
    PayloadFormat { field: &'static str },
    #[error("task failed: {reason}")]
    TaskFailed { reason: String },
    #[error("database error: {0}")]
    Database(String),
}
