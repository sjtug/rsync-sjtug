use aws_sdk_s3::{Client, Region};

use crate::opts::S3Opts;

pub async fn create_s3_client(opts: &S3Opts) -> Client {
    let shared_config = aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .region(Region::new(opts.region.clone()))
        .endpoint_url(opts.url.clone())
        .build();
    Client::from_conf(config)
}
