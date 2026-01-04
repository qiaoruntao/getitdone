use mongodb::{Client, Collection, bson::Document};

use crate::{config::Config, error::RequestError};

pub async fn connect_collection(config: &Config) -> Result<Collection<Document>, RequestError> {
    let client = Client::with_uri_str(&config.mongo_uri)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    let database = client.database(&config.database);
    Ok(database.collection(&config.collection))
}
