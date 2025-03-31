use actix_files::HttpRange;
use actix_web::body::{BoxBody, SizedStream};
use actix_web::http::header::HeaderValue;
use actix_web::http::{StatusCode, header};
use actix_web::{HttpRequest, HttpResponse};
use get_size::GetSize;
use opendal::Operator;
use rkyv::{Archive, Deserialize, Serialize};
use tracing::error;

const CHUNK_SIZE: usize = 1024 * 1024; // 1MB

#[derive(Debug, Clone, Eq, PartialEq, GetSize, Archive, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ProxyServe {
    key: String,
    content_disposition: String,
    length: u64,
}

impl ProxyServe {
    pub const fn new(key: String, content_disposition: String, length: u64) -> Self {
        Self {
            key,
            content_disposition,
            length,
        }
    }
    pub async fn serve(&self, req: &HttpRequest, op: &Operator) -> HttpResponse<BoxBody> {
        self.serve_(req, op).await.unwrap_or_else(|err| {
            error!(%err, key = %self.key, "failed to serve s3 file");
            HttpResponse::InternalServerError().finish()
        })
    }
    async fn serve_(
        &self,
        req: &HttpRequest,
        op: &Operator,
    ) -> Result<HttpResponse<BoxBody>, opendal::Error> {
        // TODO IfUnmodifiedSince and ETag
        let mut res = HttpResponse::Ok();
        res.insert_header((header::CONTENT_TYPE, "application/octet-stream"));
        res.insert_header((header::CONTENT_DISPOSITION, &*self.content_disposition));
        res.insert_header((header::ACCEPT_RANGES, "bytes"));

        let mut length = self.length;
        let mut offset = 0;

        // check for range header
        if let Some(ranges) = req.headers().get(header::RANGE) {
            if let Ok(ranges_header) = ranges.to_str() {
                if let Ok(ranges) = HttpRange::parse(ranges_header, self.length) {
                    length = ranges[0].length;
                    offset = ranges[0].start;

                    if req.headers().contains_key(&header::ACCEPT_ENCODING) {
                        // don't allow compression middleware to modify partial content
                        res.insert_header((
                            header::CONTENT_ENCODING,
                            HeaderValue::from_static("identity"),
                        ));
                    }

                    res.insert_header((
                        header::CONTENT_RANGE,
                        format!("bytes {}-{}/{}", offset, offset + length - 1, self.length),
                    ));
                } else {
                    res.insert_header((header::CONTENT_RANGE, format!("bytes */{length}")));
                    return Ok(res.status(StatusCode::RANGE_NOT_SATISFIABLE).finish());
                };
            } else {
                return Ok(res.status(StatusCode::BAD_REQUEST).finish());
            };
        };

        let reader = op.reader_with(&self.key).chunk(CHUNK_SIZE).await?;
        let reader_stream = reader.into_bytes_stream(offset..offset + length).await?;

        if offset != 0 || length != self.length {
            res.status(StatusCode::PARTIAL_CONTENT);
        }

        Ok(res.body(SizedStream::new(length, reader_stream)))
    }
}
