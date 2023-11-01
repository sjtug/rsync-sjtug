//! Rendering logic for the HTTP server.
//!
//! This module is responsible for rendering the resolved result into a HTTP response.
//!
//! TODO: too many arguments, consider grouping them into a struct.

use std::iter;
use std::time::Duration;

use actix_web::http::StatusCode;
use actix_web::web::Redirect;
use actix_web::{Either, HttpResponse, Responder};
use bstr::ByteSlice;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sailfish::TemplateOnce;
use tracing::error;
use uuid::Uuid;

use crate::path_resolve::{ListingEntry, Resolved};
use crate::pg::RevisionStat;
use crate::templates::{
    ErrorTemplate, FooterRevisionTemplate, FooterTemplate, ListingTemplate, NavbarTemplate,
    RevisionStatsTemplate,
};

impl Resolved {
    /// Render the resolved result to a HTTP response.
    pub fn to_responder(
        &self,
        req_path: &[u8],
        full_path: &str,
        prefix: &str,
        revision: i32,
        generated_at: DateTime<Utc>,
        query_time: Duration,
        locale: &str,
    ) -> impl Responder {
        match self {
            Self::Directory { entries } => {
                if !req_path.ends_with(b"/") {
                    let raw_cwd = full_path.rsplit_once('/').map_or(full_path, |(_, cwd)| cwd);
                    return Either::Right(Redirect::to(format!("{raw_cwd}/")));
                }
                let orig_entries = entries.iter().filter(|entry| entry.filename != b".");
                let parent_entry = ListingEntry {
                    filename: b"..".to_vec(),
                    len: None,
                    modify_time: None,
                    is_dir: true,
                };
                let root = req_path.is_empty() || req_path == b"/";
                let entries = if root {
                    itertools::Either::Left(orig_entries)
                } else {
                    itertools::Either::Right(iter::once(&parent_entry).chain(orig_entries))
                };
                let root_href = req_path_to_root_href(prefix, req_path);
                let title = if root {
                    format!("{prefix}/")
                } else {
                    format!("{prefix}/{}", String::from_utf8_lossy(req_path))
                };
                let components: Vec<_> = title.split('/').collect_vec();
                let navbar = comps_to_navbar(&components);
                let rendered = ListingTemplate {
                    title: &title,
                    entries,
                    navbar,
                    footer: FooterRevisionTemplate {
                        revision,
                        root_href: &root_href,
                        generated_at,
                        query_time,
                        locale,
                    },
                    locale,
                }
                .render_once()
                .expect("render must not fail");
                Either::Left(HttpResponse::Ok().content_type("text/html").body(rendered))
            }
            Self::Regular { url, expired_at: _ } => Either::Right(Redirect::to(url.to_string())),
            Self::NotFound { reason } => {
                let status = StatusCode::NOT_FOUND;
                let error_display = reason.to_string();
                let traces = reason.trace().join("\n");
                let detail = format!("{error_display}\n\nlink trace:\n{traces}");
                let root_href = req_path_to_root_href(prefix, req_path);
                let code_msg = status
                    .canonical_reason()
                    .expect("status must have a reason");
                let rendered = ErrorTemplate {
                    code: status.as_u16(),
                    code_msg,
                    navbar: NavbarTemplate {
                        components: [(&root_href, prefix)],
                        last_component: code_msg,
                        last_styles: &["text-danger", "text-uppercase"],
                    },
                    detail,
                    footer: FooterRevisionTemplate {
                        revision,
                        root_href: &root_href,
                        generated_at,
                        query_time,
                        locale,
                    },
                    locale,
                }
                .render_once()
                .expect("render must not fail");
                Either::Left(
                    HttpResponse::build(status)
                        .content_type("text/html")
                        .body(rendered),
                )
            }
        }
    }
}

/// Render the internal error into a HTTP response.
///
/// Note that we do not expose internal errors to the client, and returns a trackable id instead.
pub fn render_internal_error(
    req_path: &[u8],
    prefix: &str,
    revision: Option<i32>,
    generated_at: DateTime<Utc>,
    query_time: Duration,
    err: &eyre::Report,
    request_id: &Uuid,
    locale: &str,
) -> impl Responder {
    error!(?err, "internal error");

    let status = StatusCode::INTERNAL_SERVER_ERROR;
    let detail = format!("request id: {request_id}");
    let root_href = req_path_to_root_href(prefix, req_path);
    let code_msg = status
        .canonical_reason()
        .expect("status must have a reason");
    let navbar = NavbarTemplate {
        components: [(&root_href, prefix)],
        last_component: code_msg,
        last_styles: &["text-danger", "text-uppercase"],
    };
    let rendered = if let Some(revision) = revision {
        ErrorTemplate {
            code: status.as_u16(),
            code_msg,
            navbar,
            detail,
            footer: FooterRevisionTemplate {
                revision,
                root_href: &root_href,
                generated_at,
                query_time,
                locale,
            },
            locale,
        }
        .render_once()
        .expect("render must not fail")
    } else {
        ErrorTemplate {
            code: status.as_u16(),
            code_msg: status
                .canonical_reason()
                .expect("status must have a reason"),
            navbar,
            detail,
            footer: FooterTemplate {
                generated_at,
                query_time,
                locale,
            },
            locale,
        }
        .render_once()
        .expect("render must not fail")
    };

    HttpResponse::build(status)
        .content_type("text/html")
        .body(rendered)
}

/// Render revision stats page into a HTTP response.
pub fn render_revision_stats(
    entries: &[RevisionStat],
    generated_at: DateTime<Utc>,
    query_time: Duration,
    prefix: &str,
    locale: &str,
) -> impl Responder {
    let last_rev = entries.last().map_or(0, |entry| entry.revision);
    let rendered = RevisionStatsTemplate {
        entries: entries.iter(),
        prefix,
        footer: FooterTemplate {
            generated_at,
            query_time,
            locale,
        },
        last_rev,
        locale,
    }
    .render_once()
    .expect("render must not fail");

    HttpResponse::Ok().content_type("text/html").body(rendered)
}

fn comps_to_navbar<'a>(comps: &'a [&'a str]) -> impl TemplateOnce + 'a {
    let count = comps.len();
    let components = comps[..count - 1]
        .iter()
        .enumerate()
        .map(move |(i, component)| {
            let href = if count - i - 2 > 0 {
                iter::repeat("../").take(count - i - 2).join("")
            } else {
                ".".to_string()
            };
            (href, component)
        });
    NavbarTemplate {
        components,
        last_component: comps.last().expect("must have at least one component"),
        last_styles: &[],
    }
}

fn req_path_to_root_href(prefix: &str, req_path: &[u8]) -> String {
    let depth = if req_path == b"/" {
        0
    } else {
        req_path.find_iter(b"/").count()
    };
    if depth > 0 {
        iter::repeat("../").take(depth).join("")
    } else if req_path.is_empty() {
        prefix.to_string()
    } else {
        String::from(".")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::explicit_deref_methods, clippy::ignored_unit_patterns)]

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use actix_web::body::to_bytes;
    use actix_web::test::TestRequest;
    use actix_web::Responder;
    use chrono::{DateTime, Utc};
    use eyre::eyre;
    use once_cell::sync::Lazy;
    use proptest::arbitrary::Arbitrary;
    use proptest::prelude::TestCaseError;
    use proptest::strategy::Strategy;
    use proptest::{prop_assert, prop_assume, prop_oneof};
    use reqwest::header::HeaderMap;
    use reqwest::{header, Client};
    use serde::Deserialize;
    use test_strategy::proptest;
    use uuid::Uuid;

    use crate::path_resolve::{ListingEntry, Resolved};
    use crate::pg::RevisionStat;
    use crate::realpath::ResolveError;
    use crate::render::{render_internal_error, render_revision_stats};

    static VALIDATOR: Lazy<HtmlValidator> = Lazy::new(HtmlValidator::new);

    struct HtmlValidator {
        client: Client,
        started: AtomicBool,
    }

    #[derive(Debug, Deserialize)]
    struct VnuResult {
        messages: Vec<VnuMessage>,
    }

    #[derive(Debug, Deserialize)]
    struct VnuMessage {
        r#type: String,
        message: String,
        extract: Option<String>,
    }

    impl HtmlValidator {
        fn new() -> Self {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "text/html; charset=utf-8".parse().unwrap(),
            );
            let client = Client::builder()
                .user_agent("rsync-sjtug-test/0.1")
                .default_headers(headers)
                .build()
                .expect("build client");
            Self {
                client,
                started: Default::default(),
            }
        }

        async fn wait_for_vnu(&self) {
            if self.started.load(Ordering::Relaxed) {
                return;
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                ticker.tick().await;
                if self
                    .client
                    .get("http://localhost:8649")
                    .send()
                    .await
                    .is_ok()
                {
                    break;
                }
            }
            self.started.store(true, Ordering::Relaxed);
        }

        async fn validate(&self, src: &[u8]) -> Result<(), TestCaseError> {
            self.wait_for_vnu().await;

            let resp: VnuResult = self
                .client
                .post("http://localhost:8649")
                .query(&[("out", "json")])
                .body(src.to_vec())
                .send()
                .await
                .expect("vnu request")
                .json()
                .await
                .expect("vnu response");

            let mut explain = String::new();
            let mut pass = true;
            for msg in resp.messages {
                if msg.r#type == "error" {
                    if msg.message.contains("Forbidden code point")
                        || msg.message.contains("Saw U+0000 in stream")
                        || msg.message.contains("Astral non-character")
                    {
                        // NOTE we do not escape invalid codepoints due to performance concerns.
                        continue;
                    }

                    pass = false;
                    explain.push_str(&format!("{}: {}\n", msg.r#type, msg.message));
                    if let Some(extract) = msg.extract {
                        explain.push_str(&format!(
                            "---EXTRACT BEGIN---\n{extract}\n---EXTRACT END---\n\n"
                        ));
                    }
                }
            }
            explain.push_str(&format!(
                "---SOURCE BEGIN---\n{}\n---SOURCE END---\n\n",
                String::from_utf8_lossy(src)
            ));

            prop_assert!(pass, "vnu validation failed\n{}", explain);
            Ok(())
        }
    }

    #[allow(clippy::arc_with_non_send_sync)] // macro generated code
    fn resolved_non_regular_strategy() -> impl Strategy<Value = Resolved> {
        prop_oneof![
            Vec::<ListingEntry>::arbitrary().prop_map(|entries| Resolved::Directory { entries }),
            ResolveError::arbitrary().prop_map(|reason| Resolved::NotFound { reason })
        ]
    }

    #[proptest(async = "tokio")]
    async fn must_render_resolved_prop(
        mut req_path: Vec<u8>,
        raw_path: String,
        prefix: String,
        revision: i32,
        #[strategy(crate::tests::datetime_strategy())] generated_at: DateTime<Utc>,
        query_time: Duration,
        #[strategy(resolved_non_regular_strategy())] resolved: Resolved,
    ) {
        // ensured by validate_config
        prop_assume!(!prefix.is_empty() && !prefix.starts_with('/'));
        // no redirect
        if matches!(resolved, Resolved::Directory { .. }) {
            req_path.push(b'/');
        }

        let req = TestRequest::get().to_http_request();
        let resp = resolved
            .to_responder(
                &req_path,
                &raw_path,
                &prefix,
                revision,
                generated_at,
                query_time,
                "en",
            )
            .respond_to(&req);
        let Ok(body) = to_bytes(resp.into_body()).await else {
            panic!("must to bytes");
        };

        VALIDATOR.validate(&body).await?;
    }

    #[proptest(async = "tokio")]
    async fn must_render_internal_error_prop(
        req_path: Vec<u8>,
        prefix: String,
        revision: Option<i32>,
        #[strategy(crate::tests::datetime_strategy())] generated_at: DateTime<Utc>,
        query_time: Duration,
        err: String,
        #[strategy(proptest_arbitrary_interop::arb::< Uuid > ())] request_id: Uuid,
    ) {
        // ensured by validate_config
        prop_assume!(!prefix.is_empty() && !prefix.starts_with('/'));
        let req = TestRequest::get().to_http_request();
        let resp = render_internal_error(
            &req_path,
            &prefix,
            revision,
            generated_at,
            query_time,
            &eyre!(err),
            &request_id,
            "en",
        )
        .respond_to(&req);

        let Ok(body) = to_bytes(resp.into_body()).await else {
            panic!("must to bytes");
        };

        VALIDATOR.validate(&body).await?;
    }

    #[proptest(async = "tokio")]
    async fn must_render_revision_stats_prop(
        entries: Vec<RevisionStat>,
        prefix: String,
        #[strategy(crate::tests::datetime_strategy())] generated_at: DateTime<Utc>,
        query_time: Duration,
    ) {
        // ensured by validate_config
        prop_assume!(!prefix.is_empty() && !prefix.starts_with('/'));

        let req = TestRequest::get().to_http_request();
        let resp = render_revision_stats(&entries, generated_at, query_time, &prefix, "en")
            .respond_to(&req);

        let Ok(body) = to_bytes(resp.into_body()).await else {
            panic!("must to bytes");
        };

        VALIDATOR.validate(&body).await?;
    }
}
