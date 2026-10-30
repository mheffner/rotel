// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use serde::Serialize;
use std::error::Error;
use tower::BoxError;

use crate::exporters::xray::XRayPayload;
use crate::exporters::xray::transformer::XRayValuePayload;

const TRACES_PATH: &str = "/TraceSegments";

#[derive(Serialize)]
struct PutTraceSegments<'a> {
    #[serde(rename = "TraceSegmentDocuments")]
    trace_segment_documents: &'a [String],
}

fn build_url(endpoint: &url::Url, path: &str) -> url::Url {
    endpoint.join(path).unwrap()
}

#[derive(Clone)]
pub struct XRayRequestBuilder {
    pub base_headers: HeaderMap,
    pub uri: Uri,
}

impl XRayRequestBuilder {
    pub fn new(endpoint: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri: url::Url = match endpoint.parse() {
            Ok(u) => u,
            Err(e) => return Err(format!("failed to parse endpoint {}: {}", endpoint, e).into()),
        };

        let trace_url = build_url(&uri, TRACES_PATH);
        let uri: Uri = trace_url.to_string().parse()?;
        let mut base_headers = HeaderMap::new();
        base_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        base_headers.insert(
            "X-Amz-Target",
            HeaderValue::from_static("XRay_20160712.PutTraceSegments"),
        );
        base_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.1"),
        );

        let s = Self { uri, base_headers };
        Ok(s)
    }

    pub fn build(
        &self,
        payloads: Vec<XRayValuePayload>,
    ) -> Result<Vec<Request<XRayPayload>>, BoxError> {
        let mut requests = Vec::new();

        // Iterate through payloads and create a request for each
        for mut payload in payloads {
            // The payload.value is already an array of span documents
            // serde_json::to_string/to_vec write straight into a Vec, unlike Value's Display impl,
            // and the documents are not copied into another Value first
            let segments = if let serde_json::Value::Array(spans) = payload.value {
                spans
                    .iter()
                    .map(serde_json::to_string)
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                return Err("Expected array of spans in XRayValuePayload".into());
            };

            let data = Bytes::from(serde_json::to_vec(&PutTraceSegments {
                trace_segment_documents: &segments,
            })?);

            let mut req_builder = Request::builder().uri(&self.uri).method(Method::POST);

            let builder_headers = req_builder.headers_mut().unwrap();
            for (k, v) in self.base_headers.iter() {
                builder_headers.insert(k, v.clone());
            }

            let req = req_builder.body(Full::from(data))?;

            // Decompose the request to get parts and body
            let (parts, body) = req.into_parts();

            // Move metadata from payload into the request
            let batch_metadata = payload.metadata.take();
            let xray_payload = XRayPayload::new(body, batch_metadata);

            // Reconstruct request with the payload as the body
            let wrapped_request = Request::from_parts(parts, xray_payload);

            requests.push(wrapped_request);
        }

        Ok(requests)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serde_json::json;

    #[tokio::test]
    async fn body_matches_value_serialization() {
        let segments = json!([
            {"name": "a \"quoted\" name", "id": "0123456789abcdef", "start_time": 1.5},
            {"name": "b", "annotations": {"k": "v"}, "parent_id": "fedcba9876543210"}
        ]);
        let builder = XRayRequestBuilder::new("http://localhost:2000".to_string()).unwrap();
        let reqs = builder
            .build(vec![XRayValuePayload {
                value: segments.clone(),
                metadata: None,
            }])
            .unwrap();
        assert_eq!(reqs.len(), 1);

        let body = reqs
            .into_iter()
            .next()
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();

        // What the request body was built from before: a Value of the documents as strings
        let docs: Vec<String> = segments
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let expected = json!({ "TraceSegmentDocuments": docs }).to_string();
        assert_eq!(body, expected.as_bytes());
    }
}
