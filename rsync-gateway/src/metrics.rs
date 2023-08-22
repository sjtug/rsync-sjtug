use actix_web::web::Data;
use actix_web::Responder;
use eyre::Result;
use metrics::{describe_counter, describe_histogram, Unit};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_tracing_context::TracingContextLayer;
use metrics_util::layers::Layer;

pub const COUNTER_L1_HIT: &str = "cache_l1_hit";
pub const COUNTER_L2_HIT: &str = "cache_l2_hit";
pub const COUNTER_MISS: &str = "cache_miss";
pub const COUNTER_SMALL: &str = "cache_small_resolved_object";
pub const COUNTER_POOR: &str = "cache_poorly_compressed";
pub const HISTOGRAM_RESOLVED_SIZE: &str = "cache_resolved_size";
pub const HISTOGRAM_COMPRESSION_RATIO: &str = "cache_compression_ratio";
pub const COUNTER_RESOLVED_REGULAR: &str = "resolved_regular";
pub const COUNTER_RESOLVED_LISTING: &str = "resolved_listing";
pub const COUNTER_RESOLVED_MISSING: &str = "resolved_missing";
pub const COUNTER_RESOLVED_ERROR: &str = "resolved_error";
pub const HISTOGRAM_MISS_QUERY_TIME: &str = "miss_query_time";
pub const HISTOGRAM_L1_QUERY_TIME: &str = "l1_query_time";
pub const HISTOGRAM_L2_QUERY_TIME: &str = "l2_query_time";
const ALLOWED_NAMESPACES: &[&str] = &["namespace"];

pub fn init_metrics() -> Result<PrometheusHandle> {
    describe_counter!(COUNTER_L1_HIT, "L1 cache hit count");
    describe_counter!(COUNTER_L2_HIT, "L2 cache hit count");
    describe_counter!(COUNTER_MISS, "Cache miss count");
    describe_counter!(COUNTER_SMALL, "Small resolved object count");
    describe_counter!(COUNTER_POOR, "Poorly compressed object count");
    describe_histogram!(
        HISTOGRAM_RESOLVED_SIZE,
        Unit::Bytes,
        "Raw size of resolved object"
    );
    describe_histogram!(
        HISTOGRAM_COMPRESSION_RATIO,
        Unit::Percent,
        "Compression ratio"
    );
    describe_counter!(COUNTER_RESOLVED_REGULAR, "Resolved regular object count");
    describe_counter!(COUNTER_RESOLVED_LISTING, "Resolved directory listing count");
    describe_counter!(COUNTER_RESOLVED_MISSING, "Missing path count");
    describe_counter!(COUNTER_RESOLVED_ERROR, "Error on resolving count");
    describe_histogram!(
        HISTOGRAM_MISS_QUERY_TIME,
        Unit::Milliseconds,
        "Query time on cache miss"
    );
    describe_histogram!(
        HISTOGRAM_L1_QUERY_TIME,
        Unit::Milliseconds,
        "Query time on L1 cache hit"
    );
    describe_histogram!(
        HISTOGRAM_L2_QUERY_TIME,
        Unit::Milliseconds,
        "Query time on L2 cache hit"
    );

    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let traced_recorder = TracingContextLayer::only_allow(ALLOWED_NAMESPACES).layer(recorder);
    metrics::set_boxed_recorder(Box::new(traced_recorder))?;

    Ok(handle)
}

#[allow(clippy::unused_async)]
pub async fn metrics_handler(recorder: Data<PrometheusHandle>) -> impl Responder {
    recorder.render()
}
