// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::errors::ExporterError;
use crate::otlp::encode::OtlpEncode;
use crate::telemetry::{Counter, RotelCounter};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression as GZCompression;
use flate2::write::{GzDecoder, GzEncoder};
use opentelemetry::KeyValue;
use std::io::Write;
use tonic::Status;

// one byte for the compression flag plus four bytes for the length
const GRPC_HEADER_SIZE: usize = 5;

// cap our response buffer allocation
const GRPC_MAX_RESPONSE_SIZE: u32 = 1024 * 1024;

/// Decode a gRPC response
pub fn grpc_decode_body<T: prost::Message + Default>(
    body: Bytes,
    failed: RotelCounter<u64>,
    count: u64,
) -> Result<T, ExporterError> {
    if body.len() < GRPC_HEADER_SIZE {
        failed.add(count, &[KeyValue::new("error", "grpc.header.size")]);
        return Err(ExporterError::Generic(format!(
            "invalid response size: {}",
            body.len()
        )));
    }

    let is_gz = body.first().unwrap();
    let len = u32::from_be_bytes(body.slice(1..5).as_ref().try_into().unwrap());
    if len == 0 {
        return Ok(T::default());
    }
    if len > GRPC_MAX_RESPONSE_SIZE {
        failed.add(count, &[KeyValue::new("error", "grpc.body.size")]);
        return Err(ExporterError::Grpc(Status::failed_precondition(
            "message too large",
        )));
    }

    let body = body.slice(5..);

    if *is_gz == 0 {
        match T::decode(body) {
            Ok(r) => Ok(r),
            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "grpc.decode")]);
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
            failed.add(count, &[KeyValue::new("error", "grpc.decode.gzip")]);
            return Err(ExporterError::Generic(format!(
                "failed to gzip decode response: {}",
                e
            )));
        }

        match dec.finish() {
            Ok(buf) => match T::decode(Bytes::from(buf)) {
                Ok(r) => Ok(r),
                Err(e) => {
                    failed.add(count, &[KeyValue::new("error", "grpc.decode")]);
                    Err(ExporterError::Generic(format!(
                        "failed to decode response: {}",
                        e
                    )))
                }
            },

            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "grpc.decode.gzip")]);
                Err(ExporterError::Generic(format!(
                    "failed to finish gzip decode of response: {}",
                    e
                )))
            }
        }
    }
}

/// Encode a message as a gRPC length-prefixed message, optionally gzip compressed
pub fn grpc_encode_body<T>(msg: T, compress: bool) -> Result<Bytes, Box<dyn std::error::Error>>
where
    T: OtlpEncode,
{
    let lens = msg.measure();

    let mut buf = if compress {
        // Encode into an exactly sized buffer, then compress straight into the gRPC frame
        let mut uncompressed = Vec::with_capacity(lens.encoded_len());
        msg.encode_measured(&lens, &mut uncompressed);
        drop(msg);

        let mut buf = BytesMut::with_capacity(GRPC_HEADER_SIZE + uncompressed.len() / 4);
        buf.put_bytes(0, GRPC_HEADER_SIZE);
        let mut gz = GzEncoder::new(buf.writer(), GZCompression::default());
        if let Err(e) = gz.write_all(&uncompressed) {
            return Err(format!("unable to gzip encode buffer: {}", e).into());
        }
        match gz.finish() {
            Ok(w) => w.into_inner(),
            Err(e) => return Err(format!("unable to gzip encode buffer: {}", e).into()),
        }
    } else {
        let mut buf = BytesMut::with_capacity(GRPC_HEADER_SIZE + lens.encoded_len());
        buf.put_bytes(0, GRPC_HEADER_SIZE);
        msg.encode_measured(&lens, &mut buf);
        buf
    };

    // now we know the size of encoded message and can write the header
    let len = buf.len() - GRPC_HEADER_SIZE;
    {
        let mut buf = &mut buf[..GRPC_HEADER_SIZE];

        // compression flag, 0 means "no compression"
        buf.put_u8(compress as u8);

        buf.put_u32(len as u32);
    }

    Ok(buf.freeze())
}
