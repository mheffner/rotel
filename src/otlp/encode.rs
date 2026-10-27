// SPDX-License-Identifier: Apache-2.0

//! Protobuf encoding of OTLP export requests that produces exactly the bytes of
//! `prost::Message::encode`, but computes the length of each nested message only once.
//!
//! prost writes every embedded message as `key | length | body` and computes `length` with
//! `encoded_len()`, which walks the message's whole sub-tree. With OTLP's deep nesting
//! (request > resource > scope > span/log record/metric > data point) the innermost items are
//! therefore measured once for every enclosing level: a span four times, a metric data point six
//! times. Here a request is measured once, recording the lengths of the resource, scope, item (and
//! for metrics the metric data) levels, and then written in a second pass that reuses them.
//! Messages below the item level (attributes, events, links, exemplars, ...) are written by prost.

use bytes::BufMut;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
use prost::Message;
use prost::encoding::{
    WireType, encode_key, encode_varint, encoded_len_varint, int32, key_len, message, string,
};

/// Message lengths recorded by [`OtlpEncode::measure`], in the order the messages are written.
pub struct MeasuredLens {
    lens: Vec<usize>,
    total: usize,
}

impl MeasuredLens {
    /// Length of the encoded request
    pub fn encoded_len(&self) -> usize {
        self.total
    }
}

/// OTLP export requests that can be encoded without recomputing nested message lengths.
pub trait OtlpEncode {
    /// Compute the encoded length, recording the nested message lengths for `encode_measured`.
    fn measure(&self) -> MeasuredLens;

    /// Encode with the lengths recorded by `measure` on this same, unmodified, message. The output
    /// is identical to `prost::Message::encode`.
    fn encode_measured(&self, lens: &MeasuredLens, buf: &mut impl BufMut);

    /// Encode into a new, exactly sized, buffer.
    fn encode_to_vec_measured(&self) -> Vec<u8> {
        let lens = self.measure();
        let mut buf = Vec::with_capacity(lens.encoded_len());
        self.encode_measured(&lens, &mut buf);
        buf
    }
}

type Lens<'a> = std::slice::Iter<'a, usize>;

// Encoded size of a length-delimited field with a body of `len` bytes
fn delimited_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

fn string_len(tag: u32, value: &String) -> usize {
    if value.is_empty() {
        0
    } else {
        string::encoded_len(tag, value)
    }
}

fn encode_string(tag: u32, value: &String, buf: &mut impl BufMut) {
    if !value.is_empty() {
        string::encode(tag, value, buf);
    }
}

// Reserve the slot for a message's length before measuring its children, so that lengths are
// recorded in the order the messages are written.
fn reserve_slot(lens: &mut Vec<usize>) -> usize {
    lens.push(0);
    lens.len() - 1
}

fn write_header(tag: u32, lens: &mut Lens, buf: &mut impl BufMut) {
    let len = *lens.next().expect("message length was not measured");
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(len as u64, buf);
}

// Innermost cached level (span, log record, metric data point): measured once by prost.
fn measure_item<M: Message>(item: &M, lens: &mut Vec<usize>) -> usize {
    let len = item.encoded_len();
    lens.push(len);
    len
}

fn write_item<M: Message>(tag: u32, item: &M, lens: &mut Lens, buf: &mut impl BufMut) {
    write_header(tag, lens, buf);
    item.encode_raw(buf);
}

fn measure_items<M: Message>(tag: u32, items: &[M], lens: &mut Vec<usize>) -> usize {
    items
        .iter()
        .map(|item| delimited_len(tag, measure_item(item, lens)))
        .sum()
}

fn write_items<M: Message>(tag: u32, items: &[M], lens: &mut Lens, buf: &mut impl BufMut) {
    for item in items {
        write_item(tag, item, lens, buf);
    }
}

// ResourceSpans and ResourceLogs have the same layout: resource = 1, scopes = 2, schema_url = 3,
// and so do ScopeSpans/ScopeLogs: scope = 1, items = 2, schema_url = 3.
macro_rules! resource_scope_items {
    ($measure_res:ident, $write_res:ident, $res:ty, $scopes:ident,
     $measure_scope:ident, $write_scope:ident, $scope:ty, $items:ident,
     $measure_item:ident, $write_item:ident) => {
        fn $measure_res(res: &$res, lens: &mut Vec<usize>) -> usize {
            let slot = reserve_slot(lens);
            let mut len = res
                .resource
                .as_ref()
                .map_or(0, |r| message::encoded_len(1, r));
            for scope in &res.$scopes {
                len += delimited_len(2, $measure_scope(scope, lens));
            }
            len += string_len(3, &res.schema_url);
            lens[slot] = len;
            len
        }

        fn $write_res(tag: u32, res: &$res, lens: &mut Lens, buf: &mut impl BufMut) {
            write_header(tag, lens, buf);
            if let Some(r) = &res.resource {
                message::encode(1, r, buf);
            }
            for scope in &res.$scopes {
                $write_scope(2, scope, lens, buf);
            }
            encode_string(3, &res.schema_url, buf);
        }

        fn $measure_scope(scope: &$scope, lens: &mut Vec<usize>) -> usize {
            let slot = reserve_slot(lens);
            let mut len = scope
                .scope
                .as_ref()
                .map_or(0, |s| message::encoded_len(1, s));
            for item in &scope.$items {
                len += delimited_len(2, $measure_item(item, lens));
            }
            len += string_len(3, &scope.schema_url);
            lens[slot] = len;
            len
        }

        fn $write_scope(tag: u32, scope: &$scope, lens: &mut Lens, buf: &mut impl BufMut) {
            write_header(tag, lens, buf);
            if let Some(s) = &scope.scope {
                message::encode(1, s, buf);
            }
            for item in &scope.$items {
                $write_item(2, item, lens, buf);
            }
            encode_string(3, &scope.schema_url, buf);
        }
    };
}

resource_scope_items!(
    measure_resource_spans,
    write_resource_spans,
    ResourceSpans,
    scope_spans,
    measure_scope_spans,
    write_scope_spans,
    ScopeSpans,
    spans,
    measure_item,
    write_item
);

resource_scope_items!(
    measure_resource_logs,
    write_resource_logs,
    ResourceLogs,
    scope_logs,
    measure_scope_logs,
    write_scope_logs,
    ScopeLogs,
    log_records,
    measure_item,
    write_item
);

// ResourceMetrics/ScopeMetrics also share that layout, but the cached levels continue below the
// metric: Metric > Gauge/Sum/... > data points.
resource_scope_items!(
    measure_resource_metrics,
    write_resource_metrics,
    ResourceMetrics,
    scope_metrics,
    measure_scope_metrics,
    write_scope_metrics,
    ScopeMetrics,
    metrics,
    measure_metric,
    write_metric
);

fn measure_metric(metric: &Metric, lens: &mut Vec<usize>) -> usize {
    let slot = reserve_slot(lens);
    // Metric fields in tag order: name = 1, description = 2, unit = 3, data = 5|7|9|10|11,
    // metadata = 12
    let mut len = string_len(1, &metric.name)
        + string_len(2, &metric.description)
        + string_len(3, &metric.unit);
    if let Some(data) = &metric.data {
        len += delimited_len(data_tag(data), measure_data(data, lens));
    }
    len += message::encoded_len_repeated(12, &metric.metadata);
    lens[slot] = len;
    len
}

fn write_metric(tag: u32, metric: &Metric, lens: &mut Lens, buf: &mut impl BufMut) {
    write_header(tag, lens, buf);
    encode_string(1, &metric.name, buf);
    encode_string(2, &metric.description, buf);
    encode_string(3, &metric.unit, buf);
    if let Some(data) = &metric.data {
        write_data(data, lens, buf);
    }
    message::encode_repeated(12, &metric.metadata, buf);
}

fn data_tag(data: &Data) -> u32 {
    match data {
        Data::Gauge(_) => 5,
        Data::Sum(_) => 7,
        Data::Histogram(_) => 9,
        Data::ExponentialHistogram(_) => 10,
        Data::Summary(_) => 11,
    }
}

fn temporality_len(temporality: i32) -> usize {
    if temporality != 0 {
        int32::encoded_len(2, &temporality)
    } else {
        0
    }
}

fn encode_temporality(temporality: i32, buf: &mut impl BufMut) {
    if temporality != 0 {
        int32::encode(2, &temporality, buf);
    }
}

// Data point messages: data_points = 1, then aggregation_temporality = 2 and, for Sum,
// is_monotonic = 3
fn measure_data(data: &Data, lens: &mut Vec<usize>) -> usize {
    let slot = reserve_slot(lens);
    let len = match data {
        Data::Gauge(g) => measure_items(1, &g.data_points, lens),
        Data::Sum(s) => {
            measure_items(1, &s.data_points, lens)
                + temporality_len(s.aggregation_temporality)
                + if s.is_monotonic {
                    prost::encoding::bool::encoded_len(3, &s.is_monotonic)
                } else {
                    0
                }
        }
        Data::Histogram(h) => {
            measure_items(1, &h.data_points, lens) + temporality_len(h.aggregation_temporality)
        }
        Data::ExponentialHistogram(e) => {
            measure_items(1, &e.data_points, lens) + temporality_len(e.aggregation_temporality)
        }
        Data::Summary(s) => measure_items(1, &s.data_points, lens),
    };
    lens[slot] = len;
    len
}

fn write_data(data: &Data, lens: &mut Lens, buf: &mut impl BufMut) {
    write_header(data_tag(data), lens, buf);
    match data {
        Data::Gauge(g) => write_items(1, &g.data_points, lens, buf),
        Data::Sum(s) => {
            write_items(1, &s.data_points, lens, buf);
            encode_temporality(s.aggregation_temporality, buf);
            if s.is_monotonic {
                prost::encoding::bool::encode(3, &s.is_monotonic, buf);
            }
        }
        Data::Histogram(h) => {
            write_items(1, &h.data_points, lens, buf);
            encode_temporality(h.aggregation_temporality, buf);
        }
        Data::ExponentialHistogram(e) => {
            write_items(1, &e.data_points, lens, buf);
            encode_temporality(e.aggregation_temporality, buf);
        }
        Data::Summary(s) => write_items(1, &s.data_points, lens, buf),
    }
}

macro_rules! impl_otlp_encode {
    ($req:ty, $field:ident, $measure_res:ident, $write_res:ident) => {
        impl OtlpEncode for $req {
            fn measure(&self) -> MeasuredLens {
                let mut lens = Vec::new();
                // resource_x = 1
                let total = self
                    .$field
                    .iter()
                    .map(|res| delimited_len(1, $measure_res(res, &mut lens)))
                    .sum();
                MeasuredLens { lens, total }
            }

            fn encode_measured(&self, lens: &MeasuredLens, buf: &mut impl BufMut) {
                let mut it = lens.lens.iter();
                for res in &self.$field {
                    $write_res(1, res, &mut it, buf);
                }
                debug_assert!(it.next().is_none(), "unused message lengths");
            }
        }
    };
}

impl_otlp_encode!(
    ExportTraceServiceRequest,
    resource_spans,
    measure_resource_spans,
    write_resource_spans
);
impl_otlp_encode!(
    ExportLogsServiceRequest,
    resource_logs,
    measure_resource_logs,
    write_resource_logs
);
impl_otlp_encode!(
    ExportMetricsServiceRequest,
    resource_metrics,
    measure_resource_metrics,
    write_resource_metrics
);

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
    use opentelemetry_proto::tonic::metrics::v1::{
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, NumberDataPoint, Sum, Summary, SummaryDataPoint,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use utilities::otlp::{FakeOTLP, string_attr};

    fn assert_same_bytes<M: Message + OtlpEncode>(msg: &M) {
        let expected = msg.encode_to_vec();
        let lens = msg.measure();
        assert_eq!(expected.len(), lens.encoded_len());
        assert_eq!(expected, msg.encode_to_vec_measured());

        // also into a buffer with existing content
        let mut buf = bytes::BytesMut::from(&b"prefix"[..]);
        msg.encode_measured(&lens, &mut buf);
        assert_eq!(&buf[..6], b"prefix");
        assert_eq!(&buf[6..], &expected[..]);
    }

    #[test]
    fn traces_match_prost() {
        assert_same_bytes(&ExportTraceServiceRequest::default());
        assert_same_bytes(&FakeOTLP::trace_service_request());
        assert_same_bytes(&FakeOTLP::trace_service_request_with_spans(3, 50));

        let mut req = FakeOTLP::trace_service_request_with_spans(2, 3);
        req.resource_spans[0].resource = None;
        req.resource_spans[0].schema_url = String::new();
        req.resource_spans[1].scope_spans[0].scope = Some(InstrumentationScope {
            name: "scope".to_string(),
            version: "1.0".to_string(),
            attributes: vec![string_attr("a", "b")],
            dropped_attributes_count: 2,
        });
        req.resource_spans[1].scope_spans[0].schema_url = "https://example.com/s".to_string();
        req.resource_spans[1].scope_spans.push(Default::default());
        req.resource_spans.push(Default::default());
        assert_same_bytes(&req);
    }

    #[test]
    fn logs_match_prost() {
        assert_same_bytes(&ExportLogsServiceRequest::default());
        assert_same_bytes(&FakeOTLP::logs_service_request());
        assert_same_bytes(&FakeOTLP::logs_service_request_with_logs(4, 25));

        let mut req = FakeOTLP::logs_service_request_with_logs(2, 2);
        req.resource_logs[0].resource = None;
        req.resource_logs[1].scope_logs[0].scope = Some(InstrumentationScope {
            name: "scope".to_string(),
            ..Default::default()
        });
        req.resource_logs[1].scope_logs.push(Default::default());
        req.resource_logs.push(Default::default());
        assert_same_bytes(&req);
    }

    fn number_point(i: u64) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![string_attr("k", &i.to_string())],
            start_time_unix_nano: i,
            time_unix_nano: i + 1,
            exemplars: vec![Exemplar {
                filtered_attributes: vec![string_attr("e", "x")],
                time_unix_nano: i,
                span_id: vec![1; 8],
                trace_id: vec![2; 16],
                value: Some(
                    opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(i as i64),
                ),
            }],
            flags: (i % 2) as u32,
            value: Some(if i.is_multiple_of(2) {
                Value::AsDouble(i as f64 * 1.5)
            } else {
                Value::AsInt(-(i as i64))
            }),
        }
    }

    fn metric(name: &str, data: Option<Data>) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: "ms".to_string(),
            metadata: vec![],
            data,
        }
    }

    #[test]
    fn metrics_match_prost() {
        assert_same_bytes(&ExportMetricsServiceRequest::default());
        assert_same_bytes(&FakeOTLP::metrics_service_request());
        assert_same_bytes(&FakeOTLP::metrics_service_request_with_metrics(3, 40));

        let metrics = vec![
            metric(
                "gauge",
                Some(Data::Gauge(Gauge {
                    data_points: (0..5).map(number_point).collect(),
                })),
            ),
            metric("empty gauge", Some(Data::Gauge(Gauge::default()))),
            metric("no data", None),
            Metric {
                metadata: vec![string_attr("m", "d")],
                description: "with metadata".to_string(),
                ..metric(
                    "sum",
                    Some(Data::Sum(Sum {
                        data_points: (0..3).map(number_point).collect(),
                        aggregation_temporality: 2,
                        is_monotonic: true,
                    })),
                )
            },
            metric(
                "sum defaults",
                Some(Data::Sum(Sum {
                    data_points: vec![number_point(7)],
                    aggregation_temporality: 0,
                    is_monotonic: false,
                })),
            ),
            metric(
                "histogram",
                Some(Data::Histogram(Histogram {
                    data_points: vec![
                        HistogramDataPoint {
                            attributes: vec![string_attr("h", "1")],
                            start_time_unix_nano: 1,
                            time_unix_nano: 2,
                            count: 10,
                            sum: Some(12.5),
                            bucket_counts: vec![1, 2, 7],
                            explicit_bounds: vec![1.0, 5.0],
                            exemplars: vec![],
                            flags: 0,
                            min: Some(0.1),
                            max: Some(9.0),
                        },
                        HistogramDataPoint::default(),
                    ],
                    aggregation_temporality: 1,
                })),
            ),
            metric(
                "exp histogram",
                Some(Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        attributes: vec![string_attr("e", "1")],
                        count: 4,
                        sum: Some(3.0),
                        scale: -2,
                        zero_count: 1,
                        positive: Some(Buckets {
                            offset: -1,
                            bucket_counts: vec![1, 2],
                        }),
                        negative: None,
                        zero_threshold: 0.5,
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                })),
            ),
            metric(
                "summary",
                Some(Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 3,
                        time_unix_nano: 4,
                        count: 2,
                        sum: 3.5,
                        quantile_values: vec![ValueAtQuantile {
                            quantile: 0.5,
                            value: 1.0,
                        }],
                        flags: 1,
                    }],
                })),
            ),
        ];

        let mut req = FakeOTLP::metrics_service_request_with_metrics(1, 2);
        req.resource_metrics[0].scope_metrics[0]
            .metrics
            .extend(metrics.clone());
        req.resource_metrics.push(ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue::default()],
                dropped_attributes_count: 1,
                entity_refs: vec![],
            }),
            scope_metrics: vec![
                ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "s".to_string(),
                        ..Default::default()
                    }),
                    metrics,
                    schema_url: String::new(),
                },
                ScopeMetrics::default(),
            ],
            schema_url: "https://example.com/r".to_string(),
        });
        req.resource_metrics.push(ResourceMetrics::default());
        assert_same_bytes(&req);
    }
}
