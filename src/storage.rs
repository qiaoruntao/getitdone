use futures_util::StreamExt;
#[cfg(feature = "tracing")]
use mongodb::error::{CommandError, ErrorKind};
use mongodb::{
    Client, Collection,
    bson::{Document, doc},
};
#[cfg(feature = "tracing")]
use tracing::warn;

use crate::{config::Config, error::RequestError};

pub async fn connect_collection(config: &Config) -> Result<Collection<Document>, RequestError> {
    let client = Client::with_uri_str(&config.mongo_uri)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    let database = client.database(&config.database);
    let collection = database.collection(&config.collection);

    warn_if_missing_indexes(&collection).await;

    Ok(collection)
}

async fn warn_if_missing_indexes(collection: &Collection<Document>) {
    let mut cursor = match collection.list_indexes().await {
        Ok(cursor) => cursor,
        Err(err) => {
            #[cfg(feature = "tracing")]
            if !is_namespace_not_found(&err) {
                warn!(
                    error=%err,
                    "unable to inspect indexes; make sure task_id/status/worker_state indexes exist"
                );
            }
            return;
        }
    };

    #[cfg(feature = "tracing")]
    let mut has_task_id_unique = false;
    #[cfg(feature = "tracing")]
    let mut has_status_updated = false;
    #[cfg(feature = "tracing")]
    let mut has_worker_state = false;

    while let Some(index_result) = cursor.next().await {
        let Ok(index) = index_result else {
            #[cfg(feature = "tracing")]
            if let Err(err) = index_result {
                warn!(error=%err, "error iterating indexes");
            }
            return;
        };

        let keys = index.keys;
        if keys == doc! { "task_id": 1 } {
            let unique = index
                .options
                .as_ref()
                .and_then(|opts| opts.unique)
                .unwrap_or(false);
            if !unique {
                #[cfg(feature = "tracing")]
                warn!("task_id index exists but is not unique; idempotency keys may break");
            } else {
                #[cfg(feature = "tracing")]
                {
                    has_task_id_unique = true;
                }
            }
        } else if keys == doc! { "status": 1, "updated_at": 1 } {
            #[cfg(feature = "tracing")]
            {
                has_status_updated = true;
            }
        } else if keys == doc! { "worker_state.worker_id": 1 } {
            #[cfg(feature = "tracing")]
            {
                has_worker_state = true;
            }
        }
    }

    #[cfg(feature = "tracing")]
    {
        if !has_task_id_unique {
            warn!(
                "missing unique index on task_id; create one to enforce idempotency (db.collection.createIndex({{ task_id: 1 }}, {{ unique: true }}))"
            );
        }
        if !has_status_updated {
            warn!("missing index on {{ status: 1, updated_at: 1 }}; worker steals require a scan");
        }
        if !has_worker_state {
            warn!("missing index on worker_state.worker_id; shutdown is slower");
        }
    }
}

#[cfg(feature = "tracing")]
fn is_namespace_not_found(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Command(CommandError { code: 26, .. })
    )
}
