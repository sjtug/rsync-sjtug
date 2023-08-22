use std::fmt::{Display, Formatter};
use std::panic;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use bigdecimal::ToPrimitive;
use chrono::{DateTime, Utc};
use humansize::{ISizeFormatter, SizeFormatter, BINARY};
use sailfish::runtime::{Buffer, Render};
use sailfish::TemplateOnce;
use sqlx::postgres::types::PgInterval;
use sqlx::types::BigDecimal;

use rsync_core::pg::RevisionStatus;
use rsync_core::utils::PATH_ASCII_SET;

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
    dt.format("%Y-%m-%d %H:%M:%S")
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
    SizeFormatter::new(len, BINARY)
}

fn size_big(len: &BigDecimal) -> impl Display {
    len.to_f64().map_or(either::Either::Right("LARGE"), |len| {
        either::Either::Left(SafeFormatter::new(
            ISizeFormatter::new(len, BINARY),
            "LARGE",
        ))
    })
}

// COMMIT: crash if
/// Helper struct for formatting values that may panic.
///
/// Works around a bug in `ISizeFormatter` that causes it to panic on large values.
struct SafeFormatter<F> {
    inner: F,
    fallback: &'static str,
}

impl<F> SafeFormatter<F> {
    pub const fn new(inner: F, fallback: &'static str) -> Self {
        Self { inner, fallback }
    }
}

fn catch_unwind_silent<F: FnOnce() -> R + panic::UnwindSafe, R>(f: F) -> std::thread::Result<R> {
    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(f);
    panic::set_hook(prev_hook);
    result
}

impl<F> Display for SafeFormatter<F>
where
    F: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        catch_unwind_silent(AssertUnwindSafe(|| self.inner.fmt(f)))
            .unwrap_or_else(|_| f.write_str(self.fallback))
    }
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
}

#[derive(TemplateOnce)]
#[template(path = "footer.stpl", rm_whitespace = true)]
pub struct FooterTemplate {
    pub generated_at: DateTime<Utc>,
    pub query_time: Duration,
}

#[derive(TemplateOnce)]
#[template(path = "footer_revision.stpl", rm_whitespace = true)]
pub struct FooterRevisionTemplate<'a> {
    pub revision: i32,
    pub root_href: &'a str,
    pub generated_at: DateTime<Utc>,
    pub query_time: Duration,
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
