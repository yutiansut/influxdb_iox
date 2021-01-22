use query::{predicate::PredicateBuilder, PartitionChunk};
use read_buffer::{ColumnSelection, Database as ReadBufferDb};
use snafu::{ResultExt, Snafu};

use std::sync::{Arc, RwLock};

use super::pred::to_read_buffer_predicate;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Read Buffer Chunk Error: {}", source))]
    ReadBufferChunk { source: read_buffer::Error },

    #[snafu(display("Internal Predicate Conversion Error: {}", source))]
    InternalPredicateConversion { source: super::pred::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer {
        chunk: Arc<mutable_buffer::chunk::Chunk>,
    },
    ReadBuffer {
        db: Arc<RwLock<ReadBufferDb>>,
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
        db: Arc<RwLock<ReadBufferDb>>,
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

impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk } => chunk.id(),
            Self::ReadBuffer { chunk_id, .. } => *chunk_id,
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(&self) -> Result<Vec<data_types::partition_metadata::Table>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer { .. } => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_to_arrow(
        &self,
        dst: &mut Vec<arrow_deps::arrow::record_batch::RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> Result<(), Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => {
                chunk
                    .table_to_arrow(dst, table_name, columns)
                    .context(MutableBufferChunk)?;
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                // Translate the predicates to ReadBuffer style
                let predicate = PredicateBuilder::default().build();
                let predicate =
                    to_read_buffer_predicate(&predicate).context(InternalPredicateConversion)?;

                // translate column selection
                let column_selection = if columns.is_empty() {
                    ColumnSelection::All
                } else {
                    ColumnSelection::Some(columns)
                };

                // run the query
                let db = db.read().unwrap();
                let read_result = db
                    .read_filter(
                        partition_key,
                        table_name,
                        &[*chunk_id],
                        predicate,
                        column_selection,
                    )
                    .context(ReadBufferChunk)?;

                // copy the RecordBatches into dst
                dst.extend(read_result);
            }
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
        Ok(())
    }
}
