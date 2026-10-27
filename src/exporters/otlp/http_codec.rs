// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::errors::ExporterError;
use crate::otlp::encode::OtlpEncode;
use crate::telemetry::{Counter, RotelCounter};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression as GZCompression;
use flate2::write::{GzDecoder, GzEncoder};
use opentelemetry::KeyValue;
use std::io::Write;

pub fn http_decode_body<T: prost::Message + Default>(
    body: Bytes,
    compress: bool,
    failed: RotelCounter<u64>,
    count: u64,
) -> Result<T, ExporterError> {
    if !compress {
        match T::decode(body) {
            Ok(r) => Ok(r),
            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "http.decode")]);
                Err(ExporterError::Generic(format!(
                    "failed to decode response: {}",
                    e
                )))
            }
        }
    } else {
        let buf_out = Vec::new();
        let mut dec = GzDecoder::new(buf_out);
        if let Err(e) = dec.write_all(body.as_ref()) {
            failed.add(count, &[KeyValue::new("error", "http.decode.gzip")]);
            return Err(ExporterError::Generic(format!(
                "failed to gzip decode response: {}",
                e
            )));
        }

        match dec.finish() {
            Ok(buf) => match T::decode(Bytes::from(buf)) {
                Ok(r) => Ok(r),
                Err(e) => {
                    failed.add(count, &[KeyValue::new("error", "http.decode")]);
                    Err(ExporterError::Generic(format!(
                        "failed to decode response: {}",
                        e
                    )))
                }
            },
            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "http.decode.gzip")]);
                Err(ExporterError::Generic(format!(
                    "failed to finish gzip decode of response: {}",
                    e
                )))
            }
        }
    }
}

pub fn http_encode_body<T>(msg: T, compress: bool) -> Result<Bytes, Box<dyn std::error::Error>>
where
    T: OtlpEncode,
{
    let uncompressed = msg.encode_to_vec_measured();
    drop(msg);

    if !compress {
        return Ok(Bytes::from(uncompressed));
    }

    let mut gz = GzEncoder::new(
        BytesMut::with_capacity(uncompressed.len() / 4).writer(),
        GZCompression::default(),
    );
    if let Err(e) = gz.write_all(&uncompressed) {
        return Err(format!("unable to gzip encode buffer: {}", e).into());
    }
    match gz.finish() {
        Ok(w) => Ok(w.into_inner().freeze()),
        Err(e) => Err(format!("unable to gzip encode buffer: {}", e).into()),
    }
}
