use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use actix_web::http::StatusCode;
use actix_web::middleware::{NormalizePath, TrailingSlash};
use actix_web::web::Data;
use actix_web::{test, web, App};

use rsync_core::metadata::Metadata;
use rsync_core::tests::{generate_random_namespace, redis_client, MetadataIndex};
use rsync_core::utils::ToHex;

use crate::handler;
use crate::opts::Opts;
use crate::state::State;

#[tokio::test]
async fn integration_test() {
    let namespace = generate_random_namespace();
    let client = redis_client();

    let _guard = MetadataIndex::new(
        &client,
        &format!("{namespace}:index:42"),
        &[
            ("a/b".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20])),
            ("a/c".into(), Metadata::symlink(0, UNIX_EPOCH, "../d")),
            ("d".into(), Metadata::regular(0, UNIX_EPOCH, [1; 20])),
            ("e".into(), Metadata::symlink(0, UNIX_EPOCH, "f")),
            ("f".into(), Metadata::symlink(0, UNIX_EPOCH, "e")),
            ("g".into(), Metadata::symlink(0, UNIX_EPOCH, "h")),
        ],
    );

    let opts = Opts {
        bind: String::new(),
        s3_base: "http://s3".to_string(),
        // TODO specify redis client
        redis: "redis://localhost".parse().unwrap(),
        redis_namespace: namespace.clone(),
        update_interval: 999,
    };
    let app = test::init_service(
        App::new()
            .wrap(NormalizePath::new(TrailingSlash::MergeOnly))
            .app_data(Data::new(opts))
            .data_factory(move || {
                let namespace = namespace.clone();
                async move {
                    let client = redis_client();
                    Ok::<_, ()>(State::new(
                        client.get_multiplexed_tokio_connection().await.unwrap(),
                        namespace.clone(),
                        Arc::new(AtomicU64::new(42)),
                    ))
                }
            })
            .service(web::resource("/{path:.*}").to(handler::handler)),
    )
    .await;

    // Files that exist should be redirected to S3.
    let req = test::TestRequest::get().uri("/a/b").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get("Location").unwrap(),
        &format!("http://s3/{:x}", [0; 20].as_hex())
    );

    // Files that don't exist should return 404.
    let req = test::TestRequest::get().uri("/b").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Should follow symlinks.
    let req = test::TestRequest::get().uri("/a/c").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get("Location").unwrap(),
        &format!("http://s3/{:x}", [1; 20].as_hex())
    );

    // Circular symlinks should return 404.
    let req = test::TestRequest::get().uri("/e").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Broken symlinks should return 404.
    let req = test::TestRequest::get().uri("/g").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Listing index.
    let req = test::TestRequest::get().uri("/").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get("Location").unwrap(),
        &"http://s3/listing-42/index.html"
    );

    // Listing subdirectories.
    let req = test::TestRequest::get().uri("/a/").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get("Location").unwrap(),
        &"http://s3/listing-42/a/index.html"
    );
}
