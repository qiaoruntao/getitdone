use futures_util::StreamExt;
#[cfg(feature = "tracing")]
use mongodb::error::{CommandError, ErrorKind};
use mongodb::{
    Client, Collection,
    bson::{Document, doc},
    options::ClientOptions,
};
#[cfg(feature = "tracing")]
use tracing::warn;

use crate::{config::Config, error::RequestError};

pub async fn connect_collection(config: &Config) -> Result<Collection<Document>, RequestError> {
    #[allow(unused_mut)]
    let mut options = ClientOptions::parse(&config.mongo_uri)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    #[cfg(feature = "tracing")]
    if config.enable_metrics {
        crate::pool_metrics::install(&mut options);
    }
    let client =
        Client::with_options(options).map_err(|e| RequestError::Database(e.to_string()))?;
    let database = client.database(&config.database);
    let collection = database.collection(&config.collection);

    warn_if_missing_indexes(&collection, config.claim_sort.clone()).await;

    Ok(collection)
}

async fn warn_if_missing_indexes(collection: &Collection<Document>, claim_sort: Option<Document>) {
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
    #[cfg(feature = "tracing")]
    let mut has_claim_sort_index = false;
    let expected_claim_sort_index = expected_claim_sort_index(&claim_sort);

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
        } else if Some(&keys) == expected_claim_sort_index.as_ref() {
            #[cfg(feature = "tracing")]
            {
                has_claim_sort_index = true;
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
            warn!(
                "missing index on {{ status: 1, updated_at: 1 }}; fallback stale detection may require a scan"
            );
        }
        if !has_worker_state {
            warn!("missing index on worker_state.worker_id; shutdown is slower");
        }
        if let Some(expected) = &expected_claim_sort_index
            && !has_claim_sort_index
        {
            warn!(
                ?expected,
                "Config.claim_sort is set but no matching compound index was found; sorted \
                 claims may require an in-memory sort at scale"
            );
        }
    }
    #[cfg(not(feature = "tracing"))]
    let _ = expected_claim_sort_index;
}

#[cfg(feature = "tracing")]
fn is_namespace_not_found(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Command(CommandError { code: 26, .. })
    )
}

/// The compound index that would keep a `{status: "pending"}` claim query
/// index-covered when sorted by `sort`, e.g. `{created_at: -1}` -> `{status:
/// 1, created_at: -1}`, or `{_id: -1}` -> `{status: 1, _id: -1}`. Adapts to
/// whichever field the service's `claim_sort` actually names, rather than
/// assuming a specific one.
fn expected_claim_sort_index(sort: &Option<Document>) -> Option<Document> {
    let sort = sort.as_ref()?;
    let (field, direction) = sort.iter().next()?;
    let mut expected = Document::new();
    expected.insert("status", 1i32);
    expected.insert(field.clone(), direction.clone());
    Some(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_claim_sort_index_is_none_without_a_claim_sort() {
        assert_eq!(expected_claim_sort_index(&None), None);
    }

    #[test]
    fn expected_claim_sort_index_adapts_to_the_configured_field() {
        assert_eq!(
            expected_claim_sort_index(&Some(doc! {"created_at": -1})),
            Some(doc! {"status": 1, "created_at": -1})
        );
        assert_eq!(
            expected_claim_sort_index(&Some(doc! {"_id": -1})),
            Some(doc! {"status": 1, "_id": -1})
        );
    }
}
