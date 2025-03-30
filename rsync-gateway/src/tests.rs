use actix_web::cookie::time;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use proptest::prop_compose;
use proptest::strategy::Just;
use time::util::days_in_year;
use toml::Value;

use crate::opts::{patch_generated_config, Config};

prop_compose! {
    pub fn datetime_strategy()(
        year in 1970..=2100,
    )(
        year in Just(year),
        ord in 1..=days_in_year(year),
        secs in 0..=86399u32,
        nano in 0..=999_999_999u32  // unfortunately html time tag doesn't support leap seconds
    ) -> DateTime<Utc> {
        let date = NaiveDate::from_yo_opt(year, u32::from(ord)).expect("valid date");
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).expect("valid time");
        let dt = NaiveDateTime::new(date, time);
        DateTime::from_naive_utc_and_offset(dt, Utc)
    }
}

#[test]
fn must_generate_example_config() {
    let generated = patch_generated_config(doku::to_toml_fmt::<Config>(&doku::toml::Formatting {
        enums_style: doku::toml::EnumsStyle::Commented,
        ..Default::default()
    }));
    toml::from_str::<Value>(&generated).expect("generated config must be valid");
}

mod db_required {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;

    use actix_web::http::{StatusCode};
    use actix_web::middleware::{NormalizePath, TrailingSlash};
    use actix_web::{test, App};
    use bytesize::ByteSize;
    use chrono::DateTime;
    use eyre::Result;
    use http::{Method, Uri};
    use maplit::btreemap;
    use opendal::raw::{Access, AccessorInfo, OpPresign, PresignedRequest, RpPresign};
    use opendal::{Builder, Capability, ErrorKind, Operator, Scheme};
    use sqlx::PgPool;
    use tracing_actix_web::TracingLogger;

    use rsync_core::logging::{test_init_logger, LogFormat, LogTarget};
    use rsync_core::metadata::{MetaExtra, Metadata};
    use rsync_core::pg::{
        change_revision_status, create_revision, ensure_repository, RevisionStatus,
    };
    use rsync_core::tests::{generate_random_namespace, insert_to_revision};
    use rsync_core::utils::ToHex;

    use crate::app::configure;
    use crate::opts::{CacheOpts, Config, Endpoint, LogOpts};

    const MOCK_PRESIGN_SCHEME: Scheme = Scheme::Custom("mock-presign");

    #[derive(Debug, Default)]
    struct MockPresignBuilder {
        path_to_presign: BTreeMap<String, Uri>,
    }

    impl Builder for MockPresignBuilder {
        const SCHEME: Scheme = MOCK_PRESIGN_SCHEME;
        type Config = ();

        fn build(self) -> opendal::Result<impl Access> {
            Ok(MockPresignAccessor {
                path_to_presign: self.path_to_presign,
            })
        }
    }

    #[derive(Debug)]
    struct MockPresignAccessor {
        path_to_presign: BTreeMap<String, Uri>,
    }

    impl Access for MockPresignAccessor {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type BlockingReader = ();

        type BlockingWriter = ();
        type BlockingLister = ();
        type BlockingDeleter = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let mut am = AccessorInfo::default();
            am.set_scheme(MOCK_PRESIGN_SCHEME)
                .set_native_capability(Capability {
                    presign: true,
                    presign_write: true,
                    ..Default::default()
                });
            Arc::new(am)
        }
        async fn presign(&self, path: &str, _: OpPresign) -> opendal::Result<RpPresign> {
            self.path_to_presign.get(path).map_or_else(
                || Err(opendal::Error::new(ErrorKind::NotFound, "not found")),
                |uri| {
                    Ok(RpPresign::new(PresignedRequest::new(
                        Method::GET,
                        uri.clone(),
                        Default::default(),
                    )))
                },
            )
        }
    }

    fn mock_presign_operator(path_to_presign: BTreeMap<String, Uri>) -> Operator {
        Operator::new(MockPresignBuilder { path_to_presign })
            .expect("failed to create mock presign operator")
            .finish()
    }

    fn mock_s3_presign_map(
        namespace: &str,
        entries: &[(Vec<u8>, Metadata)],
    ) -> BTreeMap<String, Uri> {
        let mut map = BTreeMap::new();
        for (_, metadata) in entries {
            if let MetaExtra::Regular { blake2b_hash } = metadata.extra {
                map.insert(
                    format!("{namespace}/{:x}", blake2b_hash.as_hex()),
                    format!("test://{:x}", blake2b_hash.as_hex())
                        .parse()
                        .unwrap(),
                );
            }
        }
        map
    }

    // Enforcing the HRTB is necessary to avoid a lifetime error.
    const fn assert_hrtb<F: for<'a> Fn(&'a Config, &'a Endpoint) -> Result<Operator>>(f: F) -> F {
        f
    }

    macro_rules! assert_in_resp {
        ($resp: expr, $s: expr) => {
            assert!(String::from_utf8_lossy(
                &actix_web::body::to_bytes($resp.into_body()).await.unwrap()
            )
            .contains($s));
        };
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn integration_test(pool: PgPool) {
        test_init_logger();
        let mut conn = pool.acquire().await.expect("acquire");

        let namespace = generate_random_namespace();

        ensure_repository(&namespace, &mut conn)
            .await
            .expect("create repo");
        let rev = create_revision(&namespace, RevisionStatus::Partial, &mut conn)
            .await
            .expect("rev");

        let filelist = &[
            (b"a".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"a/b".to_vec(), Metadata::regular(0, UNIX_EPOCH, [0; 20])),
            (b"a/c".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "../d")),
            (b"d".to_vec(), Metadata::regular(0, UNIX_EPOCH, [1; 20])),
            (b"e".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "f")),
            (b"f".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "e")),
            (b"g".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "broken")),
            (
                "你好 世界".as_bytes().to_vec(),
                Metadata::regular(0, UNIX_EPOCH, [2; 20]),
            ),
            (
                "你好 世界2".as_bytes().to_vec(),
                Metadata::directory(0, UNIX_EPOCH),
            ),
            (
                "intérêt".as_bytes().to_vec(),
                Metadata::regular(0, UNIX_EPOCH, [3; 20]),
            ),
            (
                "intérêt2".as_bytes().to_vec(),
                Metadata::directory(0, UNIX_EPOCH),
            ),
            (b"h".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (
                b"h/i".to_vec(),
                Metadata::symlink(0, UNIX_EPOCH, "../m/n/o"),
            ),
            (b"m".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"m/n".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "../p")),
            (b"p".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"p/o".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"p/o/j".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "./")),
            (
                b"p/o/k".to_vec(),
                Metadata::symlink(0, UNIX_EPOCH, "../../v/w/"),
            ),
            (b"v".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"v/w".to_vec(), Metadata::directory(0, UNIX_EPOCH)),
            (b"v/w/a".to_vec(), Metadata::regular(0, UNIX_EPOCH, [4; 20])),
            (b"v/w/l".to_vec(), Metadata::symlink(0, UNIX_EPOCH, "a")),
            (
                b"z".to_vec(),
                Metadata::symlink(0, UNIX_EPOCH, "h/i/j/j/j/k/l"),
            ),
        ][..];

        insert_to_revision(rev, filelist, &mut conn).await;
        change_revision_status(
            rev,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &mut conn,
        )
        .await
        .expect("change rev status");

        let s3_presign_map = mock_s3_presign_map(&namespace, filelist);
        let op_builder = {
            let s3_presign_map = s3_presign_map.clone();
            move |_: &_, _: &_| Ok(mock_presign_operator(s3_presign_map.clone()))
        };

        let opts = Config {
            bind: vec![],
            log: LogOpts {
                target: LogTarget::Stderr,
                format: LogFormat::Human,
            },
            update_interval: 999,
            s3_url: String::new(),
            s3_region: String::new(),
            database_url: String::new(),
            cache: CacheOpts {
                l1_size: ByteSize::mib(32),
                l2_size: ByteSize::mib(128),
            },
            endpoints: btreemap! {
                String::from("test") =>
                Endpoint {
                    s3_bucket: String::new(),
                    s3_prefix: namespace.clone(),
                    namespace: namespace.clone(),
                    list_hidden: false
                },
            },
        };
        let (listener_handle, cfg) = configure(&opts, assert_hrtb(op_builder), pool.clone())
            .await
            .unwrap();
        let app = test::init_service(
            App::new()
                .wrap(NormalizePath::new(TrailingSlash::MergeOnly))
                .wrap(TracingLogger::default())
                .configure(cfg),
        )
        .await;

        // Files that exist should be redirected to S3.
        for (uri, hex) in [
            ("/test/a/b", [0; 20]),
            ("/test/%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C", [2; 20]),
            ("/test/int%C3%A9r%C3%AAt", [3; 20]),
        ] {
            let req = test::TestRequest::get().uri(uri).to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
            assert_eq!(
                resp.headers().get("Location").unwrap(),
                &format!("test://{:x}/", hex.as_hex())
            );
        }

        // Files that don't exist should return 404.
        let req = test::TestRequest::get().uri("/test/b").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_in_resp!(resp, "not found");

        // Should follow symlinks to files.
        for (uri, hash) in [("/test/a/c", [1; 20]), ("/test/z", [4; 20])] {
            let req = test::TestRequest::get().uri(uri).to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
            assert_eq!(
                resp.headers().get("Location").unwrap(),
                &format!("test://{:x}/", hash.as_hex())
            );
        }

        // Should follow symlinks to dirs.
        for (uri, _path) in [
            ("/test/m/n/", "/test/p/"),
            ("/test/h/i/j/j/j/k/", "/test/v/w/"),
        ] {
            let req = test::TestRequest::get().uri(uri).to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Circular symlinks should return 404.
        let req = test::TestRequest::get().uri("/test/e").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_in_resp!(resp, "too many symlinks");

        // Broken symlinks should return 404.
        let req = test::TestRequest::get().uri("/test/g").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_in_resp!(resp, "not found");

        // Listing index.
        let req = test::TestRequest::get().uri("/test/").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Listing subdirectories.
        for uri in [
            "/test/a",
            "/test/%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C2",
            "/test/int%C3%A9r%C3%AAt2",
        ] {
            let req = test::TestRequest::get()
                .uri(&format!("{uri}/"))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);

            // Redirect to trailing slash.
            let req = test::TestRequest::get().uri(uri).to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        }

        assert!(!listener_handle.is_finished(), "listener died");
    }
}
