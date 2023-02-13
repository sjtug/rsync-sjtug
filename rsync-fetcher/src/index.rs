//! Generate index page for rsync mirror.
//!
//! Adopted from [mirror-clone](https://github.com/sjtug/mirror-clone).

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::types::ByteStream;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use redis::AsyncCommands;

use rsync_core::metadata::Metadata;
use rsync_core::redis_::{follow_symlink, Target};
use rsync_core::s3::S3Opts;
use rsync_core::utils::{ToHex, PATH_ASCII_SET};

const MAX_DEPTH: usize = 64;

#[derive(Debug)]
struct IndexEntry {
    len: u64,
    modify_time: SystemTime,
    target: Target,
}

#[derive(Debug, Default)]
pub struct Index {
    prefixes: BTreeMap<String, Index>,
    objects: BTreeMap<String, IndexEntry>,
}

impl Index {
    fn insert(&mut self, path: &str, entry: IndexEntry, remaining_depth: usize) {
        if path == "." {
            return;
        }
        if remaining_depth == 0 {
            self.objects.insert(path.to_string(), entry);
        } else {
            match path.split_once('/') {
                Some((parent, rest)) => {
                    self.prefixes.entry(parent.to_string()).or_default().insert(
                        rest,
                        entry,
                        remaining_depth - 1,
                    );
                }
                None => {
                    self.objects.insert(path.to_string(), entry);
                }
            }
        }
    }

    fn snapshot(&self, prefix: &str, list_key: &str) -> Vec<String> {
        let mut result = vec![];
        result.push(format!("{prefix}{list_key}"));
        for (key, index) in &self.prefixes {
            let new_prefix = format!("{prefix}{key}/");
            result.extend(index.snapshot(&new_prefix, list_key));
        }
        result
    }

    fn generate_navbar(breadcrumb: &[&str], list_key: &str) -> String {
        let mut parent = String::new();
        let mut items = vec![];
        let mut is_first = true;
        for item in breadcrumb.iter().rev() {
            let item = html_escape::encode_text(item);
            if is_first {
                items.push(format!(
                    r#"<li class="breadcrumb-item active" aria-current="page">{item}</li>"#,
                ));
                is_first = false;
            } else {
                items.push(format!(
                    r#"<li class="breadcrumb-item"><a href="{parent}{list_key}">{item}</a></li>"#,
                ));
            }
            parent += "../";
        }
        items.reverse();
        format!(
            r#"
<nav aria-label="breadcrumb">
    <ol class="breadcrumb">
        {}
    </ol>
</nav>
        "#,
            items.join("")
        )
    }

    fn index_for(
        &self,
        prefix: &str,
        breadcrumb: &[&str],
        list_key: &str,
        now: &str,
        gateway_base: &str,
    ) -> String {
        if prefix.is_empty() {
            let mut data = String::new();

            let title = format!("Index of {}/", breadcrumb.join("/"));
            let navbar = Self::generate_navbar(breadcrumb, list_key);
            let to_root = "../".repeat(breadcrumb.len());

            let dirs = self.objects.iter().filter_map(|(k, v)| match &v.target {
                Target::Directory(path) => Some((
                    k,
                    format!("{gateway_base}{}", String::from_utf8_lossy(path)),
                    v.len,
                    v.modify_time,
                )),
                Target::File(_) => None,
            });
            let files = self.objects.iter().filter_map(|(k, v)| match &v.target {
                Target::File(hash) => Some((k, hash, v.len, v.modify_time)),
                Target::Directory(_) => None,
            });

            data += &format!(
                r#"<tr><td><a href="../{list_key}">..</a></td><td class="text-right">—</td><td class="d-none d-sm-block text-right">—</td></tr>"#
            );
            for (key, href, _, modify_time) in dirs {
                let dt = DateTime::<Utc>::from(modify_time);
                data += &format!(
                    r#"<tr><td><a href="{}">{}/</a></td><td class="text-right">—</td><td class="d-none d-sm-block text-right">{}</td></tr>"#,
                    percent_encoding::utf8_percent_encode(&href, PATH_ASCII_SET),
                    html_escape::encode_text(key),
                    dt.format("%Y-%m-%d %H:%M:%S")
                );
            }
            for (key, hash, len, modify_time) in files {
                let dt = DateTime::<Utc>::from(modify_time);
                let href = format!("{to_root}{:x}", hash.as_hex());
                data += &format!(
                    r#"<tr><td><a href="{}">{}</a></td><td class="text-right">{}</td><td class="d-none d-sm-block text-right">{}</td></tr>"#,
                    href,
                    html_escape::encode_text(key),
                    ByteSize::b(len),
                    dt.format("%Y-%m-%d %H:%M:%S")
                );
            }
            format!(
                r#"
<!doctype html>
<html>

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/4.5.3/css/bootstrap.min.css" rel="stylesheet">

    <title>{title} - SJTUG Mirror Index</title>
</head>

<body>
    <div class="container mt-3">
        {navbar}
        <table class="table table-sm table-borderless">
            <thead>
                <tr>
                    <th class="w-50" scope="col">名称</th>
                    <th class="w-25 text-right" scope="col">大小</th>
                    <th class="text-right d-none d-sm-block" scope="col">修改时间</th>
                </tr>
            </thead>
            <tbody>
                {data}
            </tbody>
        </table>
        <p class="small text-muted">该页面由 rsync-sjtug 自动生成。<a href="https://github.com/PhotonQuantum/rsync-sjtug">rsync-sjtug</a> 是 SJTUG 用于从 rsync 上游同步到对象存储的工具。</p>
        <p class="small text-muted">生成于 {now}</p>
    </div>
</body>

</html>"#,
            )
        } else if let Some((parent, rest)) = prefix.split_once('/') {
            let mut breadcrumb = breadcrumb.to_vec();
            breadcrumb.push(parent);
            self.prefixes.get(parent).unwrap().index_for(
                rest,
                &breadcrumb,
                list_key,
                now,
                gateway_base,
            )
        } else {
            panic!("unsupported prefix {prefix}");
        }
    }
}

async fn generate_index(
    redis: &redis::Client,
    redis_index: &str,
    max_depth: usize,
) -> Result<Index> {
    let mut scan_conn = redis.get_multiplexed_async_connection().await?;
    let mut hget_conn = scan_conn.clone();

    let mut index = Index::default();

    let mut files = scan_conn
        .hscan::<_, (Vec<u8>, Metadata)>(redis_index)
        .await?;
    while let Some((key, meta)) = files.next_item().await? {
        let filename = String::from_utf8_lossy(&key);

        let key = Path::new(OsStr::from_bytes(&key));
        let target = follow_symlink(&mut hget_conn, redis_index, key, Some(meta.extra)).await?;
        if let Some(target) = target {
            index.insert(
                &filename,
                IndexEntry {
                    len: meta.len,
                    modify_time: meta.modify_time,
                    target,
                },
                max_depth,
            );
        }
    }

    Ok(index)
}

pub async fn generate_index_and_upload(
    redis: &redis::Client,
    redis_namespace: &str,
    s3_client: &aws_sdk_s3::Client,
    s3_opts: &S3Opts,
    gateway_base: &str,
    repo_name: &str,
    timestamp: u64,
) -> Result<()> {
    let prefix = &s3_opts.prefix;
    let redis_index = format!("{redis_namespace}:partial");
    let index = generate_index(redis, &redis_index, MAX_DEPTH).await?;
    let keys = index.snapshot("", "index.html");

    let pb = ProgressBar::new(keys.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let now = DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(timestamp)).to_rfc2822();

    for key in keys {
        let content = index.index_for(
            key.trim_end_matches("index.html"),
            &[repo_name],
            "index.html",
            &now,
            gateway_base,
        );
        let stream = ByteStream::from(content.into_bytes());
        s3_client
            .put_object()
            .bucket(&s3_opts.bucket)
            .key(&format!("{prefix}listing-{timestamp}/{key}"))
            .content_type("text/html")
            .body(stream)
            .send()
            .await?;
        pb.inc(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use itertools::Itertools;

    use rsync_core::metadata::MetaExtra;
    use rsync_core::tests::{generate_random_namespace, redis_client, MetadataIndex};
    use rsync_core::utils::ToHex;

    use super::*;

    async fn assert_index_files(files: &[&str], expected_snapshot: &[&str], max_depth: usize) {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let test_index = format!("{namespace}:latest");
        let _guard = MetadataIndex::new(
            &client,
            &test_index,
            &files
                .iter()
                .map(|k| ((*k).to_string(), Metadata::regular(0, UNIX_EPOCH, [0; 20])))
                .collect_vec(),
        );

        let index = generate_index(&client, &test_index, max_depth)
            .await
            .unwrap();
        assert_eq!(index.snapshot("", "list.html"), expected_snapshot);
    }

    async fn assert_index(
        files: &[(&str, MetaExtra)],
        prefix: &str,
        expected_files: &[(&str, &str)],
        expected_dirs: &[(&str, &str)],
    ) {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let test_index = format!("{namespace}:latest");
        let _guard = MetadataIndex::new(
            &client,
            &test_index,
            &files
                .iter()
                .map(|(k, extra)| {
                    (
                        (*k).to_string(),
                        Metadata {
                            len: 0,
                            modify_time: UNIX_EPOCH,
                            extra: extra.clone(),
                        },
                    )
                })
                .collect_vec(),
        );

        let index = generate_index(&client, &test_index, 999).await.unwrap();
        let content = index.index_for(prefix, &["test"], "list.html", "", "http://gateway/");
        eprintln!("{}", content);

        for (key, href) in expected_files {
            let expected = format!(
                r#"<tr><td><a href="{href}">{}</a></td><td class="text-right">0 B</td><td class="d-none d-sm-block text-right">1970-01-01 00:00:00</td></tr>"#,
                html_escape::encode_text(key)
            );
            assert!(content.contains(&expected), "{expected} not found in index");
        }
        for (key, href) in expected_dirs {
            let expected = format!(
                r#"<tr><td><a href="http://gateway/{href}">{key}</a></td><td class="text-right">—</td><td class="d-none d-sm-block text-right">1970-01-01 00:00:00</td></tr>"#,
            );
            assert!(content.contains(&expected), "{expected} not found in index");
        }
    }

    #[tokio::test]
    async fn test_simple() {
        assert_index_files(&["a", "b", "c"], &["list.html"], 999).await;
    }

    #[tokio::test]
    async fn test_dir() {
        assert_index_files(
            &["a", "b", "c/a", "c/b", "c/c", "d"],
            &["list.html", "c/list.html"],
            999,
        )
        .await;
    }

    #[tokio::test]
    async fn test_dir_more() {
        assert_index_files(
            &["a", "b", "c/a/b/c/d/e"],
            &[
                "list.html",
                "c/list.html",
                "c/a/list.html",
                "c/a/b/list.html",
                "c/a/b/c/list.html",
                "c/a/b/c/d/list.html",
            ],
            999,
        )
        .await;
    }

    #[tokio::test]
    async fn test_dir_more_depth() {
        assert_index_files(
            &["a", "b", "c/a/b/c/d/e"],
            &["list.html", "c/list.html", "c/a/list.html"],
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn test_regular_files() {
        assert_index(
            &[
                ("a", MetaExtra::regular([0; 20])),
                ("b", MetaExtra::regular([1; 20])),
                ("c", MetaExtra::directory()),
                ("c/d", MetaExtra::regular([2; 20])),
            ],
            "",
            &[
                ("a", &format!("../{:x}", &[0; 20].as_hex())),
                ("b", &format!("../{:x}", &[1; 20].as_hex())),
            ],
            &[("c/", "c")],
        )
        .await;
    }

    #[tokio::test]
    async fn test_utf8() {
        assert_index(
            &[
                ("你好 世界", MetaExtra::regular([0; 20])),
                ("intérêt", MetaExtra::regular([1; 20])),
                ("你好 世界2", MetaExtra::directory()),
                ("intérêt2", MetaExtra::directory()),
            ],
            "",
            &[
                ("你好 世界", &format!("../{:x}", &[0; 20].as_hex())),
                ("intérêt", &format!("../{:x}", &[1; 20].as_hex())),
            ],
            &[
                ("你好 世界2/", "%E4%BD%A0%E5%A5%BD%20%E4%B8%96%E7%95%8C2"),
                ("intérêt2/", "int%C3%A9r%C3%AAt2"),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_link_dots() {
        assert_index(
            &[("a", MetaExtra::regular([0; 20]))],
            "",
            &[("a", &format!("../{:x}", &[0; 20].as_hex()))],
            &[],
        )
        .await;

        assert_index(
            &[("a/b", MetaExtra::regular([0; 20]))],
            "a/",
            &[("b", &format!("../../{:x}", &[0; 20].as_hex()))],
            &[],
        )
        .await;
    }

    #[tokio::test]
    async fn test_symlink() {
        assert_index(
            &[
                ("a", MetaExtra::symlink("b")),
                ("b", MetaExtra::regular([1; 20])),
            ],
            "",
            &[("b", &format!("../{:x}", &[1; 20].as_hex()))],
            &[],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follow_symlink() {
        assert_index(
            &[
                ("a/a/a/a", MetaExtra::symlink("../a")),
                ("a/a/a", MetaExtra::symlink("../a")),
                ("a/a", MetaExtra::symlink("../b/c/d")),
                ("b/c/d", MetaExtra::regular([1; 20])),
            ],
            "a/a/a/",
            &[("a", &format!("../../../../{:x}", &[1; 20].as_hex()))],
            &[],
        )
        .await;
    }

    #[tokio::test]
    async fn test_circular() {
        assert_index(
            &[
                ("a", MetaExtra::symlink("b")),
                ("b", MetaExtra::symlink("a")),
            ],
            "",
            &[],
            &[],
        )
        .await;
    }

    #[tokio::test]
    async fn test_broken() {
        assert_index(&[("a", MetaExtra::symlink("b"))], "", &[], &[]).await;
    }
}
