// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Receiver that streams PostgreSQL query rows as OTAP log records.
//!
//! The connection string is read from the configured environment variable so
//! credentials do not need to appear in pipeline YAML. This initial receiver
//! uses [`tokio_postgres::NoTls`], so the connection string must select a
//! non-TLS connection (for example, `sslmode=disable`). PostgreSQL TLS
//! connections are not yet supported.
//!
//! The configured query runs once. Its body and severity columns must be
//! non-NULL PostgreSQL text values. Rows are pulled incrementally through
//! `tokio-postgres` rather than collected into memory before OTAP batching.

mod postgres_source;

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_config::validation::validate_typed_config;
use otap_df_engine::ReceiverFactory;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::error::Error;
use otap_df_engine::local::receiver as local;
use otap_df_engine::node::NodeId;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_otap::OTAP_RECEIVER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::Arc;

use self::postgres_source::PostgresSource;
use super::log_record_source::{run_log_record_source, source_error};

/// URN for the PostgreSQL receiver.
pub const POSTGRES_RECEIVER_URN: &str = "urn:otel:receiver:postgres";

/// Configuration for the PostgreSQL receiver.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresReceiverConfig {
    /// Name of the environment variable containing the PostgreSQL connection string.
    connection_string_env: String,

    /// SQL query to execute once and stream as log records.
    query: String,

    /// Name of the non-NULL text column used as the log body.
    #[serde(default = "default_body_column")]
    body_column: String,

    /// Name of the non-NULL text column used as the log severity text.
    #[serde(default = "default_severity_text_column")]
    severity_text_column: String,

    /// Static event name assigned to every returned row.
    #[serde(default = "default_event_name")]
    event_name: String,

    /// Maximum number of log records to include in each Arrow batch.
    #[serde(default = "default_batch_size")]
    batch_size: NonZeroUsize,
}

fn default_body_column() -> String {
    "message".to_owned()
}

fn default_severity_text_column() -> String {
    "severity_text".to_owned()
}

fn default_event_name() -> String {
    "postgres.row".to_owned()
}

fn default_batch_size() -> NonZeroUsize {
    NonZeroUsize::new(1000).expect("default batch size is non-zero")
}

/// Receiver that emits one OTAP log record per PostgreSQL query row.
pub struct PostgresReceiver {
    config: PostgresReceiverConfig,
}

#[allow(unsafe_code)]
#[otap_df_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Declares the PostgreSQL receiver as a local receiver factory.
pub static POSTGRES_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: POSTGRES_RECEIVER_URN,
    create: |_pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        let config = serde_json::from_value(node_config.config.clone()).map_err(|error| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: error.to_string(),
            }
        })?;
        Ok(ReceiverWrapper::local(
            PostgresReceiver { config },
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate_typed_config::<PostgresReceiverConfig>,
};

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for PostgresReceiver {
    async fn start(
        self: Box<Self>,
        ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let batch_size = self.config.batch_size;
        let source = PostgresSource::connect(self.config)
            .await
            .map_err(|error| source_error(&effect_handler, error))?;
        run_log_record_source(source, batch_size, ctrl_chan, effect_handler).await
    }
}
