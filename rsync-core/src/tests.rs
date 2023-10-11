use crate::metadata::Metadata;
use crate::pg::insert_task;
use sqlx::{Acquire, Postgres};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[must_use]
pub fn generate_random_namespace() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time goes backwards")
        .as_secs();
    let random = rand::random::<u64>();
    format!("test_{timestamp}_{random}_!@#$%^&*()-=+")
}

pub async fn insert_to_revision<'a>(
    revision: i32,
    items: &'a [(Vec<u8>, Metadata)],
    conn: impl Acquire<'a, Database = Postgres>,
) {
    let (tx, rx) = mpsc::channel(1024);
    for item in items {
        tx.try_send(item.clone()).expect("send item");
    }
    drop(tx);

    insert_task(revision, rx, conn).await.expect("insert task");
}
