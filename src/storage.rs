use mongodb::{Client, Collection, bson::Document};

use crate::{config::Config, error::RequestError};

pub async fn connect_collection(config: &Config) -> Result<Collection<Document>, RequestError> {
    let client = Client::with_uri_str(&config.mongo_uri)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    let database = client.database(&config.database);
    let collection = database.collection(&config.collection);

    let index_model = vec![
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "task_id": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "status": 1, "updated_at": 1 })
            .build(),
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "worker_state.worker_id": 1 })
            .build(),
    ];
    collection
        .create_indexes(index_model, None)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;

    Ok(collection)
}
