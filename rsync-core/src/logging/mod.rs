use std::panic::PanicInfo;
use std::{env, panic};

use tracing::{error, Subscriber};
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{reload, EnvFilter, Layer};

pub use opts::{LogFormat, LogTarget};

mod either;
mod opts;
mod tcp;

fn tracing_panic_hook(panic_info: &PanicInfo) {
    let payload = panic_info.payload();

    let payload = payload.downcast_ref::<&str>().map_or_else(
        || payload.downcast_ref::<String>().map(String::as_str),
        |s| Some(&**s),
    );

    let location = panic_info.location().map(ToString::to_string);
    let backtrace = backtrace::Backtrace::new();

    error!(
        panic.payload = payload,
        panic.location = location,
        panic.backtrace = ?backtrace,
        "A panic occurred",
    );
}

pub struct LoggerHandle {
    f_set_target_format: Box<dyn Fn(LogTarget, LogFormat)>,
}

impl LoggerHandle {
    /// Replace log target.
    ///
    /// # Panics
    /// Panics if tracing registry is poisoned or gone.
    pub fn set_target_format(&mut self, target: LogTarget, format: LogFormat) {
        (self.f_set_target_format)(target, format);
    }
}

fn boxed_subscriber_layer<S>(
    target: LogTarget,
    format: LogFormat,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    match format {
        LogFormat::Human => {
            Box::new(tracing_subscriber::fmt::layer().with_writer(target.into_make_writer()))
        }
        LogFormat::JSON => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(target.into_make_writer()),
        ),
    }
}

/// Init logger with `tracing_subscriber`, setup eyre trace helper, metrics helper, and panic handler.
///
/// This must be called after eyre setup or panic handler will not work.
///
/// # Panics
/// Panics if tracing registry is poisoned or gone during initialization, which is unlikely.
#[allow(clippy::must_use_candidate)]
pub fn init_logger(target: LogTarget, format: LogFormat) -> LoggerHandle {
    // First load with stderr to catch errors in tcp connection
    let (subscriber, reload_handle) =
        reload::Layer::new(boxed_subscriber_layer(LogTarget::Stderr, format));

    let set_target_format = move |target: LogTarget, format: LogFormat| {
        reload_handle
            .modify(|subscriber| {
                *subscriber = boxed_subscriber_layer(target, format);
            })
            .expect("failed to replace subscriber");
    };

    let builder = tracing_subscriber::Registry::default()
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .with(subscriber);
    #[cfg(feature = "metrics-tracing-context")]
    {
        builder
            .with(metrics_tracing_context::MetricsLayer::new())
            .init();
    }
    #[cfg(not(feature = "metrics-tracing-context"))]
    {
        builder.init();
    }

    // Then try to replace with tcp.
    set_target_format(target, format);

    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        tracing_panic_hook(panic_info);
        prev_hook(panic_info);
    }));

    LoggerHandle {
        f_set_target_format: Box::new(set_target_format),
    }
}

#[cfg(feature = "tests")]
pub fn test_init_logger() {
    drop(
        tracing_subscriber::Registry::default()
            .with(tracing::level_filters::LevelFilter::DEBUG)
            .with(ErrorLayer::default())
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .try_init(),
    );
}

/// Initialize color-eyre error handling, with `NO_COLOR` support.
///
/// # Errors
/// Returns an error if `color-eyre` has already been initialized.
pub fn init_color_eyre() -> eyre::Result<()> {
    if env::var("NO_COLOR").is_ok() {
        color_eyre::config::HookBuilder::new()
            .theme(color_eyre::config::Theme::new())
            .install()?;
    } else {
        color_eyre::install()?;
    }
    Ok(())
}
