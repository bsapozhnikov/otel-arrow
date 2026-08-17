// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL adapter for the generic log-record source loop.

use async_trait::async_trait;
use futures::TryStreamExt;
use std::env::VarError;
use std::pin::Pin;
use tokio::task::{JoinError, JoinHandle};
use tokio_postgres::{Client, NoTls, RowStream};

use super::PostgresReceiverConfig;
use crate::receivers::log_record_source::{LogRecordSource, SourceLogRecord};

pub(super) struct PostgresSource {
    _client: Client,
    rows: Pin<Box<RowStream>>,
    connection_task: JoinHandle<Result<(), tokio_postgres::Error>>,
    body_column: String,
    severity_text_column: String,
    event_name: String,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PostgresSourceError {
    #[error("environment variable '{variable}' does not contain a PostgreSQL connection string")]
    ConnectionEnvironment {
        variable: String,
        #[source]
        error: VarError,
    },

    #[error("failed to connect to PostgreSQL")]
    Connect {
        #[source]
        error: tokio_postgres::Error,
    },

    #[error("failed to start PostgreSQL query")]
    Query {
        #[source]
        error: tokio_postgres::Error,
    },

    #[error("failed while streaming PostgreSQL query rows")]
    Read {
        #[source]
        error: tokio_postgres::Error,
    },

    #[error("PostgreSQL row column '{column}' must contain a non-NULL text value")]
    Column {
        column: String,
        #[source]
        error: tokio_postgres::Error,
    },

    #[error("PostgreSQL connection closed before the query stream completed")]
    ConnectionClosed,

    #[error("PostgreSQL connection failed")]
    Connection {
        #[source]
        error: tokio_postgres::Error,
    },

    #[error("PostgreSQL connection task failed")]
    ConnectionTask {
        #[source]
        error: JoinError,
    },
}

impl PostgresSource {
    pub(super) async fn connect(
        config: PostgresReceiverConfig,
    ) -> Result<Self, PostgresSourceError> {
        let connection_string = std::env::var(&config.connection_string_env).map_err(|error| {
            PostgresSourceError::ConnectionEnvironment {
                variable: config.connection_string_env.clone(),
                error,
            }
        })?;
        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .map_err(|error| PostgresSourceError::Connect { error })?;
        let connection_task = tokio::spawn(connection);
        let rows = client
            .query_raw(config.query.as_str(), std::iter::empty::<i32>())
            .await
            .map_err(|error| PostgresSourceError::Query { error })?;

        Ok(Self {
            _client: client,
            rows: Box::pin(rows),
            connection_task,
            body_column: config.body_column,
            severity_text_column: config.severity_text_column,
            event_name: config.event_name,
        })
    }

    fn map_row(&self, row: tokio_postgres::Row) -> Result<SourceLogRecord, PostgresSourceError> {
        let body = row
            .try_get::<_, String>(self.body_column.as_str())
            .map_err(|error| PostgresSourceError::Column {
                column: self.body_column.clone(),
                error,
            })?;
        let severity_text = row
            .try_get::<_, String>(self.severity_text_column.as_str())
            .map_err(|error| PostgresSourceError::Column {
                column: self.severity_text_column.clone(),
                error,
            })?;

        Ok(SourceLogRecord::new_with_severity_text(
            self.event_name.clone(),
            body,
            severity_text,
        ))
    }
}

impl Drop for PostgresSource {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

#[async_trait(?Send)]
impl LogRecordSource for PostgresSource {
    type Error = PostgresSourceError;

    async fn next_record(&mut self) -> Result<Option<SourceLogRecord>, Self::Error> {
        tokio::select! {
            row = self.rows.try_next() => {
                row.map_err(|error| PostgresSourceError::Read { error })?
                    .map(|row| self.map_row(row))
                    .transpose()
            }
            result = &mut self.connection_task => {
                match result {
                    Ok(Ok(())) => Err(PostgresSourceError::ConnectionClosed),
                    Ok(Err(error)) => Err(PostgresSourceError::Connection { error }),
                    Err(error) => Err(PostgresSourceError::ConnectionTask { error }),
                }
            }
        }
    }
}
