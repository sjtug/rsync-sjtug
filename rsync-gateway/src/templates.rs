use std::borrow::Cow;
use std::fmt::Display;
use std::time::Duration;

use bigdecimal::ToPrimitive;
use bytesize::ByteSize;
use chrono::{DateTime, SecondsFormat, Utc};
use sailfish::runtime::{Buffer, Render};
use sailfish::TemplateOnce;
use sqlx::postgres::types::PgInterval;
use sqlx::types::BigDecimal;

use rsync_core::pg::RevisionStatus;
use rsync_core::utils::PATH_ASCII_SET;
use rust_i18n::t;

use crate::path_resolve::ListingEntry;
use crate::pg::RevisionStat;

fn lossy_display(name: &[u8]) -> impl Display + '_ {
    String::from_utf8_lossy(name)
}

fn href(name: &[u8]) -> impl Display + '_ {
    percent_encoding::percent_encode(name, PATH_ASCII_SET)
}

fn href_str(name: &str) -> impl Display + '_ {
    percent_encoding::percent_encode(name.as_bytes(), PATH_ASCII_SET)
}

fn href_render(name: impl Render) -> impl Display {
    let mut buf = Buffer::new();
    name.render(&mut buf).unwrap();
    percent_encoding::percent_encode(buf.as_str().as_bytes(), PATH_ASCII_SET).to_string()
}

fn datetime(dt: DateTime<Utc>) -> impl Display {
    format!(
        r#"<time datetime="{}">{}</time>"#,
        dt.to_rfc3339_opts(SecondsFormat::Secs, false),
        dt.format("%Y-%m-%d %H:%M:%S %Z")
    )
}

fn duration(dur: Duration) -> impl Display {
    if dur.as_secs() > 0 {
        format!("{}.{:03}s", dur.as_secs(), dur.subsec_millis())
    } else if dur.subsec_millis() > 0 {
        format!("{}ms", dur.subsec_millis())
    } else if dur.subsec_micros() > 0 {
        format!("{}us", dur.subsec_micros())
    } else {
        format!("{}ns", dur.subsec_nanos())
    }
}

fn pg_interval(interval: &PgInterval) -> impl Display {
    if interval.months > 0 {
        format!("{}m", interval.months)
    } else if interval.days > 0 {
        format!("{}d", interval.days)
    } else {
        let hours = interval.microseconds / 3_600_000_000;
        let minutes = (interval.microseconds % 3_600_000_000) / 60_000_000;
        let seconds = (interval.microseconds % 60_000_000) / 1_000_000;
        if hours > 0 {
            format!("{hours}h{minutes}m{seconds}s")
        } else if minutes > 0 {
            format!("{minutes}m{seconds}s")
        } else {
            format!("{seconds}s")
        }
    }
}

fn size(len: u64) -> impl Display {
    ByteSize::b(len).to_string_as(true)
}

fn size_big(len: &BigDecimal) -> impl Display {
    len.to_u64().map_or(Cow::Borrowed("LARGE"), |len| {
        Cow::Owned(ByteSize::b(len).to_string_as(true))
    })
}

/// Template for the listing page.
#[derive(TemplateOnce)]
#[template(path = "listing.stpl", rm_whitespace = true)]
pub struct ListingTemplate<'a, I, T, N>
where
    I: Iterator<Item = &'a ListingEntry>,
    T: TemplateOnce,
    N: TemplateOnce,
{
    pub title: &'a str,
    pub entries: I,
    pub navbar: N,
    pub footer: T,
    pub locale: &'a str,
}

/// Template for the error page.
#[derive(TemplateOnce)]
#[template(path = "error.stpl", rm_whitespace = true)]
pub struct ErrorTemplate<'a, N, T>
where
    N: TemplateOnce,
    T: TemplateOnce,
{
    pub code: u16,
    pub code_msg: &'a str,
    pub detail: String,
    pub navbar: N,
    pub footer: T,
    pub locale: &'a str,
}

/// Template for revision stats page.
#[derive(TemplateOnce)]
#[template(path = "revision_stats.stpl", rm_whitespace = true)]
pub struct RevisionStatsTemplate<'a, I, T>
where
    I: Iterator<Item = &'a RevisionStat>,
    T: TemplateOnce,
{
    pub entries: I,
    pub prefix: &'a str,
    pub footer: T,
    pub locale: &'a str,
}

#[derive(TemplateOnce)]
#[template(path = "footer.stpl", rm_whitespace = true)]
pub struct FooterTemplate<'a> {
    pub generated_at: DateTime<Utc>,
    pub query_time: Duration,
    pub locale: &'a str,
}

#[derive(TemplateOnce)]
#[template(path = "footer_revision.stpl", rm_whitespace = true)]
pub struct FooterRevisionTemplate<'a> {
    pub revision: i32,
    pub root_href: &'a str,
    pub generated_at: DateTime<Utc>,
    pub query_time: Duration,
    pub locale: &'a str,
}

#[derive(TemplateOnce)]
#[template(path = "navbar.stpl", rm_whitespace = true)]
pub struct NavbarTemplate<'a, I, K, V, V2>
where
    I: IntoIterator<Item = (K, V)>,
    K: Render + 'a,
    V: Render + 'a,
    V2: Render,
{
    pub components: I,
    pub last_component: V2,
    pub last_styles: &'a [&'a str],
}
