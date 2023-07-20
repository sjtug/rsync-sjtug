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
use tracing_actix_web::RequestId;

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
        prefix: &str,
        revision: i32,
        generated_at: DateTime<Utc>,
        query_time: Duration,
    ) -> impl Responder {
        match self {
            Self::Directory { entries } => {
                let orig_entries = entries.iter().filter(|entry| entry.filename != b".");
                let parent_entry = ListingEntry {
                    filename: b"..".to_vec(),
                    len: None,
                    modify_time: None,
                    is_dir: true,
                };
                let entries = if req_path.is_empty() {
                    itertools::Either::Left(orig_entries)
                } else {
                    itertools::Either::Right(iter::once(&parent_entry).chain(orig_entries))
                };
                let cwd = if req_path.is_empty() {
                    prefix.as_bytes()
                } else {
                    req_path.rsplit_once_str(b"/").map_or(req_path, |(_, s)| s)
                };
                let root_href = req_path_to_root_href(prefix, req_path);
                let path = String::from_utf8_lossy(req_path);
                let title = if path.is_empty() {
                    prefix.to_string()
                } else {
                    format!("{prefix}/{path}")
                };
                let components: Vec<_> = title.split('/').collect_vec();
                let navbar = comps_to_navbar(&components);
                let rendered = ListingTemplate {
                    title: &title,
                    cwd,
                    entries,
                    navbar,
                    footer: FooterRevisionTemplate {
                        revision,
                        root_href: &root_href,
                        generated_at,
                        query_time,
                    },
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
                    },
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
    request_id: RequestId,
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
            },
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
            },
        }
        .render_once()
        .expect("render must not fail")
    };

    HttpResponse::build(status)
        .content_type("text/html")
        .body(rendered)
}

/// Render the internal error into a HTTP response.
///
/// Note that we do not expose internal errors to the client, and returns a trackable id instead.
pub fn render_revision_stats(
    entries: &[RevisionStat],
    generated_at: DateTime<Utc>,
    query_time: Duration,
    prefix: &str,
) -> impl Responder {
    let rendered = RevisionStatsTemplate {
        entries: entries.iter(),
        prefix,
        footer: FooterTemplate {
            generated_at,
            query_time,
        },
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
    let depth = req_path.find_iter(b"/").count();
    if depth > 0 {
        iter::repeat("../").take(depth).join("")
    } else if req_path.is_empty() {
        prefix.to_string()
    } else {
        String::from(".")
    }
}
