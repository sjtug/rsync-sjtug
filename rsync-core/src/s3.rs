use aws_sdk_s3::{Client, Region};
use url::Url;

#[derive(Debug, Clone)]
pub struct S3Opts {
    pub region: String,
    pub url: Url,
    pub bucket: String,
    // With end slash.
    pub prefix: String,
}

pub async fn create_s3_client(opts: &S3Opts) -> Client {
    let shared_config = aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .region(Region::new(opts.region.clone()))
        .endpoint_url(opts.url.clone())
        .build();
    Client::from_conf(config)
}

#[cfg(feature = "s3-tests")]
pub mod tests {
    use aws_config::SdkConfig;
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration};
    use aws_sdk_s3::types::ByteStream;
    use aws_sdk_s3::{Client, Region};
    use s3s::auth::SimpleAuth;
    use s3s::service::S3Service;
    use s3s_aws::Connector;
    use s3s_fs::FileSystem;
    use tempfile::TempDir;
    use url::Url;

    use crate::s3::S3Opts;

    const DOMAIN_NAME: &str = "localhost:8014";

    #[must_use]
    pub fn s3_service() -> (TempDir, Client) {
        let cred = Credentials::for_tests();

        let temp_dir = TempDir::new().expect("temp dir");

        let fs = FileSystem::new(temp_dir.path()).expect("initialize s3 fs");
        let auth = SimpleAuth::from_single(cred.access_key_id(), cred.secret_access_key());

        let mut service = S3Service::new(Box::new(fs));
        service.set_auth(Box::new(auth));
        service.set_base_domain(DOMAIN_NAME);

        let conn = Connector::from(service.into_shared());

        let cfg = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_connector(conn)
            .region(Region::new("test"))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build();

        (temp_dir, Client::new(&cfg))
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn with_s3(objects: &[(String, Vec<u8>)]) -> (TempDir, Client, S3Opts) {
        let (guard, client) = s3_service();

        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from("test"))
            .build();

        client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket("test")
            .send()
            .await
            .expect("create bucket");

        for (key, content) in objects {
            let body = ByteStream::from(content.clone());
            client
                .put_object()
                .bucket("test")
                .key(key)
                .body(body)
                .send()
                .await
                .expect("put object");
        }

        let opts = S3Opts {
            region: "test".to_string(),
            url: Url::parse("http://localhost:8014").unwrap(),
            bucket: "test".to_string(),
            prefix: "test/".to_string(),
        };

        (guard, client, opts)
    }
}
