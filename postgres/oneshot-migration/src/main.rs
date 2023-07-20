use sqlx::{Connection, PgConnection};
use std::env;

#[tokio::main]
async fn main() {
    let mut conn = PgConnection::connect(&env::var("DATABASE_URL").expect("missing DATABASE_URL"))
        .await
        .expect("failed to connect to database");
    sqlx::migrate!("../migrations")
        .run(&mut conn)
        .await
        .expect("failed to run migrations");
}
