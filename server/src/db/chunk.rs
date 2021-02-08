use arrow_deps::{
    datafusion::{logical_plan::LogicalPlan, physical_plan::SendableRecordBatchStream},
    util::str_iter_to_batch,
};
use data_types::{schema::Schema, selection::Selection};
use mutable_buffer::chunk::Chunk as MBChunk;
use query::{predicate::Predicate, util::make_scan_plan, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use snafu::{ResultExt, Snafu};

use std::sync::Arc;

use super::{
    pred::to_read_buffer_predicate,
    streams::{MutableBufferChunkStream, ReadFilterResultsStream},
};

use async_trait::async_trait;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunk {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Internal Predicate Conversion Error: {}", source))]
    InternalPredicateConversion { source: super::pred::Error },

    #[snafu(display("internal error creating plan: {}", source))]
    InternalPlanCreation {
        source: arrow_deps::datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion {
        source: arrow_deps::arrow::error::ArrowError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer {
        chunk: Arc<MBChunk>,
    },
    ReadBuffer {
        db: Arc<ReadBufferDb>,
        partition_key: String,
        chunk_id: u32,
    },
    ParquetFile, // TODO add appropriate type here
}

impl DBChunk {
    /// Create a new mutable buffer chunk
    pub fn new_mb(chunk: Arc<mutable_buffer::chunk::Chunk>) -> Arc<Self> {
        Arc::new(Self::MutableBuffer { chunk })
    }

    /// create a new read buffer chunk
    pub fn new_rb(
        db: Arc<ReadBufferDb>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Arc<Self> {
        let partition_key = partition_key.into();
        Arc::new(Self::ReadBuffer {
            db,
            chunk_id,
            partition_key,
        })
    }
}

#[async_trait]
impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk } => chunk.id(),
            Self::ReadBuffer { chunk_id, .. } => *chunk_id,
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(
        &self,
    ) -> Result<Vec<data_types::partition_metadata::TableSummary>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer { .. } => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    async fn table_names(&self, predicate: &Predicate) -> Result<LogicalPlan, Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => {
                let names: Vec<Option<&str>> = if chunk.is_empty() {
                    Vec::new()
                } else {
                    let chunk_predicate = chunk
                        .compile_predicate(predicate)
                        .context(MutableBufferChunk)?;

                    chunk
                        .table_names(&chunk_predicate)
                        .context(MutableBufferChunk)?
                        .into_iter()
                        .map(Some)
                        .collect()
                };

                let batch = str_iter_to_batch("tables", names).context(ArrowConversion)?;

                make_scan_plan(batch).context(InternalPlanCreation)
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;

                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(InternalPredicateConversion)?;

                let batch = db
                    .table_names(partition_key, &[chunk_id], rb_predicate)
                    .context(ReadBufferChunk { chunk_id })?;
                make_scan_plan(batch).context(InternalPlanCreation)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented")
            }
        }
    }

    async fn table_schema(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<Schema, Self::Error> {
        match self {
            DBChunk::MutableBuffer { chunk } => chunk
                .table_schema(table_name, selection)
                .context(MutableBufferChunk),
            DBChunk::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;

                // TODO: Andrew -- I think technically this reordering
                // should be happening inside the read buffer, but
                // we'll see when we get to read_filter as the same
                // issue will appear when actually reading columns
                // back
                let needs_sort = matches!(selection, Selection::All);

                // For now, since read_filter is evaluated lazily,
                // "run" a query with no predicates simply to get back the
                // schema
                let predicate = read_buffer::Predicate::default();
                let mut schema = db
                    .read_filter(partition_key, table_name, &[chunk_id], predicate, selection)
                    .context(ReadBufferChunk { chunk_id })?
                    .schema()
                    .context(ReadBufferChunk { chunk_id })?;

                // Ensure the order of the output columns is as
                // specified
                if needs_sort {
                    schema = schema.sort_fields_by_name()
                }

                Ok(schema)
            }
            DBChunk::ParquetFile => {
                unimplemented!("parquet file not implemented for table schema")
            }
        }
    }

    async fn has_table(&self, table_name: &str) -> bool {
        match self {
            DBChunk::MutableBuffer { chunk } => chunk.has_table(table_name).await,
            DBChunk::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                db.has_table(partition_key, table_name, &[chunk_id])
            }
            DBChunk::ParquetFile => {
                unimplemented!("parquet file not implemented for has_table")
            }
        }
    }

    async fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        match self {
            DBChunk::MutableBuffer { chunk } => {
                // Note Mutable buffer doesn't support predicate
                // pushdown (other than pruning out the entire chunk
                // via `might_pass_predicate)
                let schema = self
                    .table_schema(table_name, selection.clone())
                    .await?
                    .into();
                Ok(Box::pin(MutableBufferChunkStream::new(
                    chunk.clone(),
                    schema,
                    table_name,
                    selection,
                )))
            }
            DBChunk::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(InternalPredicateConversion)?;
                let read_results = db
                    .read_filter(
                        partition_key,
                        table_name,
                        &[chunk_id],
                        rb_predicate,
                        selection,
                    )
                    .context(ReadBufferChunk { chunk_id })?;

                let schema = read_results
                    .schema()
                    .context(ReadBufferChunk { chunk_id })?
                    .into();

                Ok(Box::pin(ReadFilterResultsStream::new(read_results, schema)))
            }
            DBChunk::ParquetFile => {
                unimplemented!("parquet file not implemented for scan_data")
            }
        }
    }

    fn could_pass_predicate(&self, _predicate: &Predicate) -> Result<bool> {
        match self {
            Self::MutableBuffer { .. } => {
                // For now, we might get an error if we try and
                // compile a chunk predicate (e.g. for tables that
                // don't exist in the chunk) which could signal the
                // chunk can't pass the predicate.

                // However, we can also get an error if there is some
                // unsupported operation and we need a way to
                // distinguish the two cases.
                Ok(true)
            }
            Self::ReadBuffer { .. } => {
                // TODO: ask Edd how he wants this wired up in the read buffer
                Ok(true)
            }
            Self::ParquetFile => {
                // TODO proper filtering for parquet files
                Ok(true)
            }
        }
    }
}
