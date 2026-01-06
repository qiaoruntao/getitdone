use std::time::Duration;

use getitdone::{Caller, Config};
use mongodb::{Client, bson::doc};
use uuid::Uuid;

async fn raw_config() -> Config {
    let collection = format!("index_tests_{}", Uuid::new_v4());
    Config::builder()
        .mongo_uri("mongodb://localhost:27017")
        .database("getitdone_index_tests")
        .collection(&collection)
        .worker_switch_timeout(Duration::from_secs(1))
        .build()
}

async fn drop_collection(config: &Config) {
    if let Ok(client) = Client::with_uri_str(&config.mongo_uri).await {
        let _ = client
            .database(&config.database)
            .collection::<mongodb::bson::Document>(&config.collection)
            .drop(None)
            .await;
    }
}

#[tokio::test]
async fn warn_if_indexes_missing_on_existing_collection() {
    let config = raw_config().await;

    // Pre-create the collection with only the default _id index.
    let client = Client::with_uri_str(&config.mongo_uri).await.unwrap();
    let collection = client
        .database(&config.database)
        .collection::<mongodb::bson::Document>(&config.collection);
    collection
        .insert_one(doc! { "bootstrap": true }, None)
        .await
        .unwrap();
    collection.delete_many(doc! {}, None).await.unwrap();

    // Caller::connect triggers the warn_if_missing_indexes path.
    let _caller = Caller::connect(config.clone()).await.unwrap();

    drop_collection(&config).await;
}

#[tokio::test]
async fn warn_if_indexes_skips_when_collection_absent() {
    let config = raw_config().await;
    // No collection exists yet, so list_indexes returns NamespaceNotFound.
    let _caller = Caller::connect(config.clone()).await.unwrap();
    drop_collection(&config).await;
}
