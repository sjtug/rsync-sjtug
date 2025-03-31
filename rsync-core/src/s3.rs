use eyre::Result;
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer, TracingLayer};
use opendal::services::S3;

#[derive(Debug, Clone)]
pub struct S3Opts {
    pub region: String,
    pub url: String,
    pub bucket: String,
}

/// Build S3 operator.
///
/// # Errors
/// Returns error if failed to build operator.
pub fn build_operator(opts: &S3Opts) -> Result<Operator> {
    let builder = S3::default()
        .endpoint(&opts.url)
        .region(&opts.region)
        .bucket(&opts.bucket);
    Ok(Operator::new(builder)?
        .layer(TimeoutLayer::new())
        .layer(RetryLayer::new())
        .layer(TracingLayer)
        .finish())
}
