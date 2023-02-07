use std::time::{SystemTime, UNIX_EPOCH};

use redis::{Commands, Connection};

use crate::metadata::Metadata;

#[must_use]
pub fn redis_client() -> redis::Client {
    // TODO specify redis client
    redis::Client::open("redis://localhost").expect("redis")
}

#[must_use]
pub fn generate_random_namespace() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time goes backwards")
        .as_secs();
    let random = rand::random::<u64>();
    format!("test_{timestamp}_{random}")
}

pub struct MetadataIndex {
    conn: Connection,
    key: String,
}

impl Drop for MetadataIndex {
    fn drop(&mut self) {
        let _: () = self.conn.del(&self.key).unwrap();
    }
}

impl MetadataIndex {
    #[must_use]
    pub fn new(client: &redis::Client, key: &str, items: &[(String, Metadata)]) -> Self {
        let mut conn = client.get_connection().expect("redis");
        let temp_key = format!("temp_key:{key}");

        let mut pipe = redis::pipe();
        for (path, metadata) in items {
            pipe.hset(&temp_key, path, metadata);
        }

        pipe.query::<()>(&mut conn).expect("redis query");

        if !items.is_empty() {
            let _: () = conn.rename(temp_key.as_str(), key).expect("redis rename");
        }

        Self {
            conn,
            key: key.to_string(),
        }
    }
}
