// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared ingestion loop and OTAP encoder for log-record sources.

use async_trait::async_trait;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::{Error, ReceiverErrorKind, format_error_sources};
use otap_df_engine::local::receiver as local;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_otap::pdata::{Context, OtapPdata};
use otap_df_pdata::encode::record::logs::LogsRecordBatchBuilder;
use otap_df_pdata::otap::{Logs, OtapArrowRecords};
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source-neutral data needed to construct one log record.
pub(crate) struct SourceLogRecord {
    event_name: String,
    body: String,
    severity_number: Option<i32>,
    severity_text: String,
}

impl SourceLogRecord {
    /// Creates an informational source log record.
    pub(crate) fn new(event_name: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            event_name: event_name.into(),
            body: body.into(),
            severity_number: Some(9),
            severity_text: "INFO".to_owned(),
        }
    }

    /// Creates a source log record with source-provided severity text.
    #[cfg(feature = "postgres-receiver")]
    pub(crate) fn new_with_severity_text(
        event_name: impl Into<String>,
        body: impl Into<String>,
        severity_text: impl Into<String>,
    ) -> Self {
        Self {
            event_name: event_name.into(),
            body: body.into(),
            severity_number: None,
            severity_text: severity_text.into(),
        }
    }
}

/// Asynchronous source of individual log records.
#[async_trait(?Send)]
pub(crate) trait LogRecordSource {
    /// Error produced while reading from the source.
    type Error: StdError + 'static;

    /// Returns the next record, or `None` when the source is exhausted.
    async fn next_record(&mut self) -> Result<Option<SourceLogRecord>, Self::Error>;
}

/// Runs a log-record source with bounded OTAP batching and engine control handling.
pub(crate) async fn run_log_record_source<S>(
    mut source: S,
    batch_size: NonZeroUsize,
    mut ctrl_chan: local::ControlChannel<OtapPdata>,
    effect_handler: local::EffectHandler<OtapPdata>,
) -> Result<TerminalState, Error>
where
    S: LogRecordSource,
{
    let mut pending = Vec::with_capacity(batch_size.get());
    let mut read_complete = false;

    loop {
        tokio::select! {
            biased;

            message = ctrl_chan.recv() => {
                if handle_control(message?, &mut pending, &effect_handler).await? {
                    return Ok(TerminalState::default());
                }
            }

            record = source.next_record(), if !read_complete => {
                match record.map_err(|error| source_error(&effect_handler, error))? {
                    Some(record) => {
                        pending.push(record);
                        if pending.len() == batch_size.get() {
                            send_records(&mut pending, &effect_handler).await?;
                        }
                    }
                    None => {
                        send_records(&mut pending, &effect_handler).await?;
                        read_complete = true;
                    }
                }
            }
        }
    }
}

/// Converts a source-specific error into the engine's receiver error.
pub(crate) fn source_error(
    effect_handler: &local::EffectHandler<OtapPdata>,
    error: impl StdError + 'static,
) -> Error {
    let error_message = error.to_string();
    let source_detail = format_error_sources(&error);
    Error::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind: ReceiverErrorKind::Transport,
        error: error_message,
        source_detail,
    }
}

async fn handle_control(
    message: NodeControlMsg<OtapPdata>,
    pending: &mut Vec<SourceLogRecord>,
    effect_handler: &local::EffectHandler<OtapPdata>,
) -> Result<bool, Error> {
    match message {
        NodeControlMsg::DrainIngress { .. } => {
            send_records(pending, effect_handler).await?;
            effect_handler.notify_receiver_drained().await?;
            Ok(true)
        }
        NodeControlMsg::Shutdown { .. } => Ok(true),
        _ => Ok(false),
    }
}

async fn send_records(
    records: &mut Vec<SourceLogRecord>,
    effect_handler: &local::EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if records.is_empty() {
        return Ok(());
    }

    let pdata = build_otap_logs(records).map_err(|error| Error::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind: ReceiverErrorKind::Other,
        error,
        source_detail: String::new(),
    })?;
    effect_handler.send_message_with_source_node(pdata).await?;
    records.clear();
    Ok(())
}

fn build_otap_logs(records: &[SourceLogRecord]) -> Result<OtapPdata, String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let timestamp = i64::try_from(timestamp).unwrap_or(i64::MAX);
    let mut logs = LogsRecordBatchBuilder::new();

    for (index, record) in records.iter().enumerate() {
        let id = u16::try_from(index)
            .map_err(|_| "log batch contains more than 65536 records".to_owned())?;

        logs.append_id(Some(id));
        logs.append_time_unix_nano(timestamp);
        logs.append_observed_time_unix_nano(timestamp);
        logs.append_severity_number(record.severity_number);
        logs.append_severity_text(Some(record.severity_text.as_bytes()));
        logs.body.append_str(record.body.as_bytes());
        logs.append_schema_url(None);
        logs.append_dropped_attributes_count(0);
        logs.append_flags(None);
        logs.append_trace_id(None)
            .map_err(|error| error.to_string())?;
        logs.append_span_id(None)
            .map_err(|error| error.to_string())?;
        logs.append_event_name(Some(record.event_name.as_bytes()));
    }

    let record_count = records.len();
    logs.resource.append_id_n(0, record_count);
    logs.resource.append_schema_url_n(None, record_count);
    logs.resource
        .append_dropped_attributes_count_n(0, record_count);

    logs.scope.append_id_n(0, record_count);
    logs.scope.append_name_n(None, record_count);
    logs.scope.append_version_n(None, record_count);
    logs.scope
        .append_dropped_attributes_count_n(0, record_count);

    let mut otap_records = OtapArrowRecords::Logs(Logs::default());
    let records = logs.finish().map_err(|error| error.to_string())?;
    otap_records
        .set(ArrowPayloadType::Logs, records)
        .map_err(|error| error.to_string())?;

    Ok(OtapPdata::new(Context::default(), otap_records.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::OtapPayload;

    /// Scenario: A source provides two generic log records.
    /// Guarantees: The OTAP logs batch contains one Arrow row per source record.
    #[test]
    fn builds_one_arrow_row_per_source_record() {
        let records = vec![
            SourceLogRecord::new("first", "first body"),
            SourceLogRecord::new("second", "second body"),
        ];
        let pdata = build_otap_logs(&records).expect("build source records");
        let OtapPayload::OtapArrowRecords(records) = pdata.payload() else {
            panic!("expected an OTAP Arrow payload");
        };

        let logs = records
            .get(ArrowPayloadType::Logs)
            .expect("logs record batch");
        assert_eq!(logs.num_rows(), 2);
    }
}
