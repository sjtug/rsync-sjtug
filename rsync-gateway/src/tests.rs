use std::time::UNIX_EPOCH;

use actix_web::http::StatusCode;
use actix_web::middleware::{NormalizePath, TrailingSlash};
use actix_web::{test, App};
use maplit::btreemap;

use rsync_core::metadata::Metadata;
use rsync_core::tests::{generate_random_namespace, redis_client, MetadataIndex};
use rsync_core::utils::{test_init_logger, ToHex};

use crate::app::configure;
use crate::opts::{Endpoint, Opts};

#[tokio::test]
async fn integration_test() {
    test_init_logger();

    let namespace = generate_random_namespace();
    let client = redis_client();

    let _guard = MetadataIndex::new(
        &client,
        &format!("{namespace}:index:42"),
        &[
            ("a".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("a/b".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20])),
            ("a/c".into(), Metadata::symlink(0, UNIX_EPOCH, "../d")),
            ("d".into(), Metadata::regular(0, UNIX_EPOCH, [1; 20])),
            ("e".into(), Metadata::symlink(0, UNIX_EPOCH, "f")),
            ("f".into(), Metadata::symlink(0, UNIX_EPOCH, "e")),
            ("g".into(), Metadata::symlink(0, UNIX_EPOCH, "broken")),
            (
                "你好 世界".into(),
                Metadata::regular(0, UNIX_EPOCH, [2; 20]),
            ),
            ("你好 世界2".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("intérêt".into(), Metadata::regular(0, UNIX_EPOCH, [3; 20])),
            ("intérêt2".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("h".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("h/i".into(), Metadata::symlink(0, UNIX_EPOCH, "../m/n/o")),
            ("m".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("m/n".into(), Metadata::symlink(0, UNIX_EPOCH, "../p")),
            ("p".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("p/o".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("p/o/j".into(), Metadata::symlink(0, UNIX_EPOCH, "./")),
            (
                "p/o/k".into(),
                Metadata::symlink(0, UNIX_EPOCH, "../../v/w/"),
            ),
            ("v".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("v/w".into(), Metadata::directory(0, UNIX_EPOCH)),
            ("v/w/a".into(), Metadata::regular(0, UNIX_EPOCH, [4; 20])),
            ("v/w/l".into(), Metadata::symlink(0, UNIX_EPOCH, "a")),
            (
                "z".into(),
                Metadata::symlink(0, UNIX_EPOCH, "h/i/j/j/j/k/l"),
            ),
        ],
    );

    let redis_url = option_env!("TEST_REDIS_URL").unwrap_or("redis://localhost");
    let opts = Opts {
        bind: vec![],
        update_interval: 999,
        endpoints: btreemap! {
            String::new() =>
            Endpoint {
                s3_website: "http://s3".to_string(),
                redis: redis_url.parse().unwrap(),
                redis_namespace: namespace.clone(),
            },
        },
    };
    let cfg = configure(&opts).await.unwrap();
    let app = test::init_service(
        App::new()
            .wrap(NormalizePath::new(TrailingSlash::Trim))
            .configure(cfg),
    )
    .await;

    // Files that exist should be redirected to S3.
    for (uri, hex) in [
        ("/a/b", [0; 20]),
        ("/%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C", [2; 20]),
        ("/int%C3%A9r%C3%AAt", [3; 20]),
    ] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            &format!("http://s3/{:x}", hex.as_hex())
        );
    }

    // Files that don't exist should return 404.
    let req = test::TestRequest::get().uri("/b").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Should follow symlinks to files.
    for (uri, hash) in [("/a/c", [1; 20]), ("/z", [4; 20])] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            &format!("http://s3/{:x}", hash.as_hex())
        );
    }

    // Should follow symlinks to dirs.
    for (uri, path) in [("/m/n", "p"), ("/h/i/j/j/j/k", "v/w")] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            &format!("http://s3/listing-42/{path}/index.html")
        );
    }

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
    for (uri, href) in [
        ("/a", "a/index.html"),
        (
            "/%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C2",
            "%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C2/index.html",
        ),
        ("/int%C3%A9r%C3%AAt2", "int%C3%A9r%C3%AAt2/index.html"),
    ] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            &format!("http://s3/listing-42/{href}")
        );
    }
}
