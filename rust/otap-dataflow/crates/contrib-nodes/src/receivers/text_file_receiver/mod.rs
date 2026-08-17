// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Receiver that emits each line of a text file as an OTAP log record.

mod text_file_source;

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
use std::path::PathBuf;
use std::sync::Arc;

use self::text_file_source::TextFileSource;
use super::log_record_source::{run_log_record_source, source_error};

/// URN for the text file receiver.
pub const TEXT_FILE_RECEIVER_URN: &str = "urn:otel:receiver:text_file";

/// Configuration for the text file receiver.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextFileReceiverConfig {
    /// Path of the text file to read.
    path: PathBuf,

    /// Maximum number of log records to include in each Arrow batch.
    #[serde(default = "default_batch_size")]
    batch_size: NonZeroUsize,
}

fn default_batch_size() -> NonZeroUsize {
    NonZeroUsize::new(1000).expect("default batch size is non-zero")
}

/// Receiver that emits one log record per line of a text file.
pub struct TextFileReceiver {
    config: TextFileReceiverConfig,
}

#[allow(unsafe_code)]
#[otap_df_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Declares the text file receiver as a local receiver factory.
pub static TEXT_FILE_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: TEXT_FILE_RECEIVER_URN,
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
            TextFileReceiver { config },
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate_typed_config::<TextFileReceiverConfig>,
};

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for TextFileReceiver {
    async fn start(
        self: Box<Self>,
        ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let source = TextFileSource::open(self.config.path)
            .await
            .map_err(|error| source_error(&effect_handler, error))?;
        run_log_record_source(source, self.config.batch_size, ctrl_chan, effect_handler).await
    }
}
