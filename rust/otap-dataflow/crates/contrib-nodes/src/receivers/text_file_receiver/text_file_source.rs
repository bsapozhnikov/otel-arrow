// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Text-file adapter for the generic log-record source loop.

use async_trait::async_trait;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, BufReader};

use super::super::log_record_source::{LogRecordSource, SourceLogRecord};

pub(super) struct TextFileSource {
    path: PathBuf,
    lines: tokio::io::Lines<BufReader<tokio::fs::File>>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TextFileSourceError {
    #[error("failed to {operation} text file '{}'", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
}

impl TextFileSource {
    pub(super) async fn open(path: PathBuf) -> Result<Self, TextFileSourceError> {
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| TextFileSourceError::Io {
                operation: "open",
                path: path.clone(),
                error,
            })?;
        Ok(Self {
            path,
            lines: BufReader::new(file).lines(),
        })
    }
}

#[async_trait(?Send)]
impl LogRecordSource for TextFileSource {
    type Error = TextFileSourceError;

    async fn next_record(&mut self) -> Result<Option<SourceLogRecord>, Self::Error> {
        self.lines
            .next_line()
            .await
            .map(|line| line.map(|body| SourceLogRecord::new("text.file.line", body)))
            .map_err(|error| TextFileSourceError::Io {
                operation: "read",
                path: self.path.clone(),
                error,
            })
    }
}
