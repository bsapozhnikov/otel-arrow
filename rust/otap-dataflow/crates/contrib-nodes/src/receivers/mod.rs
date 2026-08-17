// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

mod log_record_source;

/// ETW (Event Tracing for Windows) receiver.
#[cfg(all(feature = "etw-receiver", target_os = "windows"))]
pub mod etw_receiver;

/// Kafka receiver.
#[cfg(feature = "kafka-receiver")]
pub mod kafka_receiver;

/// PostgreSQL receiver.
#[cfg(feature = "postgres-receiver")]
pub mod postgres_receiver;

/// Text file receiver.
pub mod text_file_receiver;

/// Linux user_events receiver.
#[cfg(all(feature = "user_events-receiver", target_os = "linux"))]
pub mod user_events_receiver;
