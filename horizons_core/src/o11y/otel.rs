use crate::o11y::traits::{O11yExporter, OtelSpan, OtelStatusCode, OtlpProtocol};
use crate::{Error, Result};
use async_trait::async_trait;
use prost::Message;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone)]
pub struct OtlpExporter {
    endpoint: String,
    protocol: OtlpProtocol,
    service_name: String,
    headers: BTreeMap<String, String>,
    timeout: Duration,
    http: reqwest::Client,
}

impl OtlpExporter {
    #[tracing::instrument(level = "debug")]
    pub fn new(
        endpoint: &str,
        protocol: OtlpProtocol,
        service_name: String,
        headers: BTreeMap<String, String>,
        timeout: Duration,
    ) -> Result<Self> {
        if endpoint.trim().is_empty() {
            return Err(Error::InvalidInput("otlp endpoint is empty".to_string()));
        }
        if service_name.trim().is_empty() {
            return Err(Error::InvalidInput("service_name is empty".to_string()));
        }

        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::backend("build reqwest client", e))?;

        Ok(Self {
            endpoint: endpoint.trim().to_string(),
            protocol,
            service_name,
            headers,
            timeout,
            http,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn to_request(&self, spans: &[OtelSpan]) -> Result<otlp::ExportTraceServiceRequest> {
        let resource = otlp::Resource {
            attributes: vec![otlp::KeyValue {
                key: "service.name".to_string(),
                value: Some(otlp::AnyValue {
                    value: Some(otlp::any_value::Value::StringValue(
                        self.service_name.clone(),
                    )),
                }),
            }],
            dropped_attributes_count: 0,
        };

        let mut out_spans = Vec::with_capacity(spans.len());
        for s in spans {
            out_spans.push(span_to_proto(s)?);
        }

        Ok(otlp::ExportTraceServiceRequest {
            resource_spans: vec![otlp::ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![otlp::ScopeSpans {
                    scope: Some(otlp::InstrumentationScope {
                        name: "horizons".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans: out_spans,
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn export_http(&self, req: otlp::ExportTraceServiceRequest) -> Result<()> {
        let url = if self.endpoint.ends_with("/v1/traces") {
            self.endpoint.clone()
        } else {
            format!("{}/v1/traces", self.endpoint.trim_end_matches('/'))
        };

        let mut body = Vec::new();
        req.encode(&mut body)
            .map_err(|e| Error::backend("encode otlp request", e))?;

        let mut request = self
            .http
            .post(url)
            .header("content-type", "application/x-protobuf")
            .body(body);
        for (k, v) in &self.headers {
            request = request.header(k, v);
        }

        let resp = request
            .send()
            .await
            .map_err(|e| Error::backend("send otlp http request", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "otlp http export failed: {status} {text}"
            )));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn export_grpc(&self, req: otlp::ExportTraceServiceRequest) -> Result<()> {
        let ep = self.endpoint.trim_end_matches('/').to_string();
        let mut endpoint = tonic::transport::Endpoint::from_shared(ep)
            .map_err(|e| Error::backend("parse otlp grpc endpoint", e))?;
        endpoint = endpoint.timeout(self.timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Error::backend("connect otlp grpc", e))?;

        let mut grpc = tonic::client::Grpc::new(channel);
        let mut tonic_req = tonic::Request::new(req);
        for (k, v) in &self.headers {
            let key = tonic::metadata::MetadataKey::from_bytes(k.as_bytes())
                .map_err(|_| Error::InvalidInput(format!("invalid grpc metadata key: {k}")))?;
            let val = tonic::metadata::MetadataValue::try_from(v.as_str())
                .map_err(|_| Error::InvalidInput(format!("invalid grpc metadata value for {k}")))?;
            tonic_req.metadata_mut().insert(key, val);
        }

        grpc.unary::<otlp::ExportTraceServiceRequest, otlp::ExportTraceServiceResponse, _>(
            tonic_req,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .map_err(|e| Error::backend("otlp grpc export", e))?;

        Ok(())
    }
}

#[async_trait]
impl O11yExporter for OtlpExporter {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn export(&self, spans: Vec<OtelSpan>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        let req = self.to_request(&spans)?;
        match self.protocol {
            OtlpProtocol::HttpProtobuf => self.export_http(req).await,
            OtlpProtocol::Grpc => self.export_grpc(req).await,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "otlp"
    }
}

fn span_to_proto(s: &OtelSpan) -> Result<otlp::Span> {
    let trace_id = decode_hex_exact(&s.trace_id, 16, "trace_id")?;
    let span_id = decode_hex_exact(&s.span_id, 8, "span_id")?;
    let parent_span_id = match &s.parent_span_id {
        None => vec![],
        Some(pid) => decode_hex_exact(pid, 8, "parent_span_id")?,
    };

    let start = to_unix_nanos(s.start_time)?;
    let end = to_unix_nanos(s.end_time)?;

    let mut attrs = Vec::with_capacity(s.attributes.len());
    for (k, v) in &s.attributes {
        if v.is_null() {
            continue;
        }
        attrs.push(otlp::KeyValue {
            key: k.clone(),
            value: Some(any_value_from_json(v)),
        });
    }

    let mut events = Vec::with_capacity(s.events.len());
    for ev in &s.events {
        events.push(otlp::span::Event {
            time_unix_nano: to_unix_nanos(ev.timestamp)?,
            name: ev.name.clone(),
            attributes: ev
                .attributes
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| otlp::KeyValue {
                    key: k.clone(),
                    value: Some(any_value_from_json(v)),
                })
                .collect(),
            dropped_attributes_count: 0,
        });
    }

    let status = Some(otlp::Status {
        message: s.status.message.clone().unwrap_or_default(),
        code: match s.status.code {
            OtelStatusCode::Unset => 0,
            OtelStatusCode::Ok => 1,
            OtelStatusCode::Error => 2,
        },
    });

    Ok(otlp::Span {
        trace_id,
        span_id,
        trace_state: "".to_string(),
        parent_span_id,
        name: s.name.clone(),
        kind: 1, // INTERNAL
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        attributes: attrs,
        dropped_attributes_count: 0,
        events,
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status,
    })
}

fn any_value_from_json(v: &serde_json::Value) -> otlp::AnyValue {
    use otlp::any_value::Value;
    let value = match v {
        serde_json::Value::Null => return otlp::AnyValue { value: None },
        serde_json::Value::Bool(b) => Value::BoolValue(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::IntValue(i)
            } else if let Some(f) = n.as_f64() {
                Value::DoubleValue(f)
            } else {
                Value::StringValue(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::StringValue(s.clone()),
        serde_json::Value::Array(a) => {
            let vals = a
                .iter()
                .filter(|e| !e.is_null())
                .map(|e| any_value_from_json(e))
                .collect();
            Value::ArrayValue(otlp::ArrayValue { values: vals })
        }
        serde_json::Value::Object(_) => Value::StringValue(v.to_string()),
    };
    otlp::AnyValue { value: Some(value) }
}

fn to_unix_nanos(ts: chrono::DateTime<chrono::Utc>) -> Result<u64> {
    let secs = ts.timestamp();
    let nanos = ts.timestamp_subsec_nanos() as i64;
    if secs < 0 {
        return Err(Error::InvalidInput(
            "timestamp before unix epoch".to_string(),
        ));
    }
    let total = (secs as u128)
        .checked_mul(1_000_000_000u128)
        .and_then(|v| v.checked_add(nanos as u128))
        .ok_or_else(|| Error::InvalidInput("timestamp out of range".to_string()))?;
    Ok(total as u64)
}

fn decode_hex_exact(s: &str, nbytes: usize, what: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() != nbytes * 2 {
        return Err(Error::InvalidInput(format!(
            "{what} must be {} hex chars, got {}",
            nbytes * 2,
            s.len()
        )));
    }
    let mut out = vec![0u8; nbytes];
    let bytes = s.as_bytes();
    for i in 0..nbytes {
        out[i] = (hex_val(bytes[2 * i], what)? << 4) | hex_val(bytes[2 * i + 1], what)?;
    }
    Ok(out)
}

fn hex_val(b: u8, what: &str) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::InvalidInput(format!("{what} is not valid hex"))),
    }
}

// Minimal OTLP trace proto structs (subset of opentelemetry-proto).
pub(crate) mod otlp {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportTraceServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_spans: ::prost::alloc::vec::Vec<ResourceSpans>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportTraceServiceResponse {}

    #[derive(Clone, PartialEq, Message)]
    pub struct ResourceSpans {
        #[prost(message, optional, tag = "1")]
        pub resource: ::core::option::Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_spans: ::prost::alloc::vec::Vec<ScopeSpans>,
        #[prost(string, tag = "3")]
        pub schema_url: ::prost::alloc::string::String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ScopeSpans {
        #[prost(message, optional, tag = "1")]
        pub scope: ::core::option::Option<InstrumentationScope>,
        #[prost(message, repeated, tag = "2")]
        pub spans: ::prost::alloc::vec::Vec<Span>,
        #[prost(string, tag = "3")]
        pub schema_url: ::prost::alloc::string::String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct InstrumentationScope {
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Resource {
        #[prost(message, repeated, tag = "1")]
        pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
        #[prost(uint32, tag = "2")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Span {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: ::prost::alloc::string::String,
        #[prost(bytes = "vec", tag = "4")]
        pub parent_span_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(string, tag = "5")]
        pub name: ::prost::alloc::string::String,
        #[prost(enumeration = "SpanKind", tag = "6")]
        pub kind: i32,
        #[prost(uint64, tag = "7")]
        pub start_time_unix_nano: u64,
        #[prost(uint64, tag = "8")]
        pub end_time_unix_nano: u64,
        #[prost(message, repeated, tag = "9")]
        pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
        #[prost(uint32, tag = "10")]
        pub dropped_attributes_count: u32,
        #[prost(message, repeated, tag = "11")]
        pub events: ::prost::alloc::vec::Vec<span::Event>,
        #[prost(uint32, tag = "12")]
        pub dropped_events_count: u32,
        #[prost(message, repeated, tag = "13")]
        pub links: ::prost::alloc::vec::Vec<span::Link>,
        #[prost(uint32, tag = "14")]
        pub dropped_links_count: u32,
        #[prost(message, optional, tag = "15")]
        pub status: ::core::option::Option<Status>,
    }

    pub mod span {
        use super::*;
        #[derive(Clone, PartialEq, Message)]
        pub struct Event {
            #[prost(uint64, tag = "1")]
            pub time_unix_nano: u64,
            #[prost(string, tag = "2")]
            pub name: ::prost::alloc::string::String,
            #[prost(message, repeated, tag = "3")]
            pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
            #[prost(uint32, tag = "4")]
            pub dropped_attributes_count: u32,
        }

        #[derive(Clone, PartialEq, Message)]
        pub struct Link {}
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Status {
        #[prost(string, tag = "2")]
        pub message: ::prost::alloc::string::String,
        #[prost(enumeration = "StatusCode", tag = "3")]
        pub code: i32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct KeyValue {
        #[prost(string, tag = "1")]
        pub key: ::prost::alloc::string::String,
        #[prost(message, optional, tag = "2")]
        pub value: ::core::option::Option<AnyValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct AnyValue {
        #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
        pub value: ::core::option::Option<any_value::Value>,
    }

    pub mod any_value {
        use super::*;
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Value {
            #[prost(string, tag = "1")]
            StringValue(::prost::alloc::string::String),
            #[prost(bool, tag = "2")]
            BoolValue(bool),
            #[prost(int64, tag = "3")]
            IntValue(i64),
            #[prost(double, tag = "4")]
            DoubleValue(f64),
            #[prost(message, tag = "5")]
            ArrayValue(ArrayValue),
            #[prost(message, tag = "6")]
            KvlistValue(KeyValueList),
            #[prost(bytes, tag = "7")]
            BytesValue(::prost::alloc::vec::Vec<u8>),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ArrayValue {
        #[prost(message, repeated, tag = "1")]
        pub values: ::prost::alloc::vec::Vec<AnyValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct KeyValueList {
        #[prost(message, repeated, tag = "1")]
        pub values: ::prost::alloc::vec::Vec<KeyValue>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SpanKind {
        Unspecified = 0,
        Internal = 1,
        Server = 2,
        Client = 3,
        Producer = 4,
        Consumer = 5,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        Unset = 0,
        Ok = 1,
        Error = 2,
    }
}
