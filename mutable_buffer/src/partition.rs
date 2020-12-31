//! Holds one or more Chunks.

use arrow_deps::arrow::record_batch::RecordBatch;
use generated_types::wal as wb;
use std::{collections::BTreeMap, sync::Arc};

use crate::chunk::{Chunk, Error as ChunkError};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error writing to active chunk of partition with key '{}': {}",
        partition_key,
        source
    ))]
    WritingChunkData {
        partition_key: String,
        source: ChunkError,
    },

    #[snafu(display(
        "Can not drop active chunk '{}' of partition with key '{}'",
        chunk_id,
        partition_key,
    ))]
    DropMutableChunk {
        partition_key: String,
        chunk_id: u64,
    },

    #[snafu(display(
        "Can not drop unknown chunk '{}' of partition with key '{}'. Valid chunk ids: {:?}",
        chunk_id,
        partition_key,
        valid_chunk_ids,
    ))]
    DropUnknownChunk {
        partition_key: String,
        chunk_id: u64,
        valid_chunk_ids: Vec<u64>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    /// The partition key that is shared by all Chunks in this Partition
    key: String,

    /// The active mutable Chunk; All new writes go to this chunk
    mutable_chunk: Chunk,

    /// Immutable chunks which can no longer be written
    /// key: chunk_id, value: Chunk
    ///
    /// Use BTreeMap here as it is ordered by id and we need to use
    /// `iter()` to iterate over chunks in their creation order
    immutable_chunks: BTreeMap<u64, Arc<Chunk>>,

    /// Responsible for assigning ids to chunks. Eventually, this might
    /// need to start at a number other than 0.
    id_generator: u64,
}

impl Partition {
    pub fn new(key: impl Into<String>) -> Self {
        // TODO: for existing partitions, does this need to pick up at preexisting ID?
        let mut id_generator = 0;

        let key: String = key.into();
        let mutable_chunk = Chunk::new(&key, id_generator);
        id_generator += 1;

        Self {
            key,
            mutable_chunk,
            immutable_chunks: BTreeMap::new(),
            id_generator,
        }
    }

    /// write data to the active mutable chunk
    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        assert_eq!(
            entry
                .partition_key()
                .expect("partition key should be present"),
            self.key
        );
        self.mutable_chunk
            .write_entry(entry)
            .with_context(|| WritingChunkData {
                partition_key: entry.partition_key().unwrap(),
            })
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> crate::chunk::Result<()> {
        for chunk in self.iter() {
            chunk.table_to_arrow(dst, table_name, columns)?
        }
        Ok(())
    }

    /// Return information about the chunks held in this partition
    #[allow(dead_code)]
    pub fn chunk_info(&self) -> PartitionChunkInfo {
        PartitionChunkInfo {
            num_immutable_chunks: self.immutable_chunks.len(),
        }
    }

    /// roll over the active chunk, adding it to immutable_chunks if
    /// it had data, and returns it.
    ///
    /// Any new writes to this partition will go to a new chunk.
    ///
    /// Queries will continue to see data in the specified chunk until
    /// it is dropped.
    pub fn rollover_chunk(&mut self) -> Arc<Chunk> {
        let chunk_id = self.id_generator;
        self.id_generator += 1;
        let mut chunk = Chunk::new(self.key(), chunk_id);
        std::mem::swap(&mut chunk, &mut self.mutable_chunk);
        chunk.mark_immutable();
        let chunk = Arc::new(chunk);
        if !chunk.is_empty() {
            let existing_value = self.immutable_chunks.insert(chunk.id(), chunk.clone());
            assert!(existing_value.is_none());
        }
        chunk
    }

    /// Drop the specified chunk for the partition, returning a reference to the
    /// chunk
    #[allow(dead_code)]
    pub fn drop_chunk(&mut self, chunk_id: u64) -> Result<Arc<Chunk>> {
        self.immutable_chunks.remove(&chunk_id).ok_or_else(|| {
            let partition_key = self.key.clone();
            if self.mutable_chunk.id() == chunk_id {
                Error::DropMutableChunk {
                    partition_key,
                    chunk_id,
                }
            } else {
                let valid_chunk_ids: Vec<u64> = self.iter().map(|c| c.id()).collect();
                Error::DropUnknownChunk {
                    partition_key,
                    chunk_id,
                    valid_chunk_ids,
                }
            }
        })
    }

    /// Return the partition key shared by all data stored in this
    /// partition
    pub fn key(&self) -> &str {
        &self.key
    }

    /// in Return an iterator over each Chunk in this partition
    pub fn iter(&self) -> ChunkIter<'_> {
        ChunkIter::new(self)
    }
}

/// information on chunks for this partition
#[derive(Debug, Default, PartialEq)]
pub struct PartitionChunkInfo {
    pub num_immutable_chunks: usize,
}

/// Iterates over chunks in a partition. Always iterates over mutable
/// partition last so that chunks are visited in the same order even
/// after rollover. This results in data being output in the same
/// order it was written in
pub struct ChunkIter<'a> {
    partition: &'a Partition,
    visited_mutable: bool,
    immutable_iter: std::collections::btree_map::Iter<'a, u64, Arc<Chunk>>,
}

impl<'a> ChunkIter<'a> {
    fn new(partition: &'a Partition) -> Self {
        let immutable_iter = partition.immutable_chunks.iter();
        Self {
            partition,
            visited_mutable: false,
            immutable_iter,
        }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.partition;

        self.immutable_iter
            .next()
            .map(|(_k, v)| v.as_ref())
            .or_else(|| {
                if !self.visited_mutable {
                    self.visited_mutable = true;
                    Some(&partition.mutable_chunk)
                } else {
                    None
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use data_types::data::split_lines_into_write_entry_partitions;

    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, test_util::sort_record_batch,
    };
    use influxdb_line_protocol::parse_lines;

    #[tokio::test]
    async fn test_rollover_chunk() {
        let mut partition = Partition::new("a_key");

        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "h2o,state=MA,city=Boston temp=71.4 200",
            ],
        )
        .await;

        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 71.4 | 200  |",
            "+--------+-------+------+------+",
        ];
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 0
            }
        );
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));

        println!("rolling over chunk");

        // now rollover chunk, and expected results should be the same
        let chunk = partition.rollover_chunk();
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 1
            }
        );
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(row_count("h2o", &chunk), 2);

        // calling rollover chunk again is ok; It is returned but not added to the
        // immutable chunk list
        let chunk = partition.rollover_chunk();
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 1
            }
        );
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(row_count("h2o", &chunk), 0);
    }

    #[tokio::test]
    async fn test_rollover_chunk_new_data_visible() {
        let mut partition = Partition::new("a_key");

        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "h2o,state=MA,city=Boston temp=71.4 200",
            ],
        )
        .await;

        // now rollover chunk
        let chunk = partition.rollover_chunk();
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 1
            }
        );
        assert_eq!(row_count("h2o", &chunk), 2);

        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=69.0 50",
                "h2o,state=MA,city=Boston temp=72.3 300",
                "h2o,state=MA,city=Boston temp=73.2 400",
            ],
        )
        .await;

        // note the rows come out in the order they were written in
        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 71.4 | 200  |",
            "| Boston | MA    | 69   | 50   |",
            "| Boston | MA    | 72.3 | 300  |",
            "| Boston | MA    | 73.2 | 400  |",
            "+--------+-------+------+------+",
        ];
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));

        // now rollover chunk again
        let chunk = partition.rollover_chunk();
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 2
            }
        );
        assert_eq!(row_count("h2o", &chunk), 3);
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
    }

    #[tokio::test]
    async fn test_rollover_chunk_multiple_tables() {
        let mut partition = Partition::new("a_key");

        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "o2,state=MA,city=Boston temp=71.4 100",
                "o2,state=MA,city=Boston temp=72.4 200",
            ],
        )
        .await;

        let expected_h2o = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "+--------+-------+------+------+",
        ];
        let expected_o2 = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 71.4 | 100  |",
            "| Boston | MA    | 72.4 | 200  |",
            "+--------+-------+------+------+",
        ];
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 0
            }
        );

        assert_table_eq!(expected_h2o, &dump_table(&partition, "h2o"));
        assert_table_eq!(expected_o2, &dump_table(&partition, "o2"));

        // now rollover chunk again
        let chunk = partition.rollover_chunk();
        assert_eq!(
            partition.chunk_info(),
            PartitionChunkInfo {
                num_immutable_chunks: 1
            }
        );
        assert_eq!(row_count("h2o", &chunk), 1);
        assert_eq!(row_count("o2", &chunk), 2);

        assert_table_eq!(expected_h2o, &dump_table(&partition, "h2o"));
        assert_table_eq!(expected_o2, &dump_table(&partition, "o2"));
    }

    #[tokio::test]
    async fn test_rollover_chunk_ids() {
        let mut partition = Partition::new("a_key");

        // When the chunk is rolled over, it gets id 0
        let chunk = partition.rollover_chunk();
        assert_eq!(chunk.id(), 0);

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=70.4 100"]).await;

        let chunk = partition.rollover_chunk();
        assert_eq!(chunk.id(), 1);

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=71.4 200"]).await;

        let chunk = partition.rollover_chunk();
        assert_eq!(chunk.id(), 2);

        assert_eq!(all_ids_with_data(&partition), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_rollover_chunk_drop_data_is_gone() {
        let mut partition = Partition::new("a_key");

        // Given data loaded into two chunks (one immutable)
        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "h2o,state=MA,city=Boston temp=72.4 200",
            ],
        )
        .await;

        // When the chunk is rolled over
        partition.rollover_chunk();

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=71.4 100"]).await;

        // Initially, data from both chunks appear in queries
        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 72.4 | 200  |",
            "| Boston | MA    | 71.4 | 100  |",
            "+--------+-------+------+------+",
        ];
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(all_ids_with_data(&partition), vec![0, 1]);

        // When the first chunk is dropped
        partition.drop_chunk(0).unwrap();

        // then it no longer is in the
        // partitions is left

        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 71.4 | 100  |",
            "+--------+-------+------+------+",
        ];
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(all_ids_with_data(&partition), vec![1]);
    }

    #[tokio::test]
    async fn test_write_after_drop_chunk() {
        let mut partition = Partition::new("a_key");

        // Given data loaded into three chunks (two immutable
        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=70.4 100"]).await;
        partition.rollover_chunk();

        // Given data loaded into three chunks (two immutable
        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=72.4 200"]).await;
        partition.rollover_chunk();

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=71.4 300"]).await;
        partition.rollover_chunk();

        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 72.4 | 200  |",
            "| Boston | MA    | 71.4 | 300  |",
            "+--------+-------+------+------+",
        ];
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(all_ids_with_data(&partition), vec![0, 1, 2]);

        // when one chunk is dropped and new data is added
        partition.drop_chunk(1).unwrap();

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=73.0 400"]).await;

        // then the results reflect that

        let expected = &[
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 71.4 | 300  |",
            "| Boston | MA    | 73   | 400  |",
            "+--------+-------+------+------+",
        ];
        assert_table_eq!(expected, &dump_table(&partition, "h2o"));
        assert_eq!(all_ids_with_data(&partition), vec![0, 2, 3]);
    }

    #[tokio::test]
    async fn test_drop_chunk_invalid() {
        let mut partition = Partition::new("a_key");
        let e = partition.drop_chunk(0).unwrap_err();
        assert_eq!(
            "Can not drop active chunk '0' of partition with key 'a_key'",
            format!("{}", e)
        );

        load_data(&mut partition, &["h2o,state=MA,city=Boston temp=70.4 100"]).await;
        partition.rollover_chunk();
        partition.drop_chunk(0).unwrap(); // drop is ok
                                          // can't drop again
        let e = partition.drop_chunk(0).unwrap_err();
        assert_eq!(
            "Can not drop unknown chunk '0' of partition with key 'a_key'. Valid chunk ids: [1]",
            format!("{}", e)
        );
    }

    #[tokio::test]
    async fn test_chunk_timestamps() {
        let start = Utc::now();
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        // Given data loaded into two chunks
        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "o2,state=MA,city=Boston temp=71.4 100",
                "o2,state=MA,city=Boston temp=72.4 200",
            ],
        )
        .await;
        let after_data_load = Utc::now();

        // When the chunk is rolled over
        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();

        println!("start: {:?}, after_partition_creation: {:?}, after_data_load: {:?}, after_rollover: {:?}",
                 start, after_partition_creation, after_data_load, after_rollover);
        println!("Chunk: {:#?}", chunk);

        // then the chunk creation and rollover times are as expected
        assert!(start < chunk.time_of_first_write.unwrap());
        assert!(after_partition_creation < chunk.time_of_first_write.unwrap());
        assert!(chunk.time_of_first_write.unwrap() < after_data_load);
        assert!(chunk.time_of_first_write.unwrap() == chunk.time_of_last_write.unwrap());
        assert!(after_data_load < chunk.time_became_immutable.unwrap());
        assert!(chunk.time_became_immutable.unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_last_write() {
        let mut partition = Partition::new("a_key");

        // Given data loaded into two chunks
        load_data(&mut partition, &["o2,state=MA,city=Boston temp=71.4 100"]).await;
        let after_data_load_1 = Utc::now();

        load_data(&mut partition, &["o2,state=MA,city=Boston temp=72.4 200"]).await;
        let after_data_load_2 = Utc::now();
        let chunk = partition.rollover_chunk();

        assert!(chunk.time_of_first_write.unwrap() < after_data_load_1);
        assert!(chunk.time_of_first_write.unwrap() < chunk.time_of_last_write.unwrap());
        assert!(chunk.time_of_last_write.unwrap() < after_data_load_2);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_empty() {
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();
        assert!(chunk.time_of_first_write.is_none());
        assert!(chunk.time_of_last_write.is_none());
        assert!(after_partition_creation < chunk.time_became_immutable.unwrap());
        assert!(chunk.time_became_immutable.unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_empty_write() {
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        // Call load data but don't write any actual data (aka it was an empty write)
        load_data(&mut partition, &[""]).await;

        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();

        assert!(chunk.time_of_first_write.is_none());
        assert!(chunk.time_of_last_write.is_none());
        assert!(after_partition_creation < chunk.time_became_immutable.unwrap());
        assert!(chunk.time_became_immutable.unwrap() < after_rollover);
    }

    fn row_count(table_name: &str, chunk: &Chunk) -> u32 {
        let stats = chunk.table_stats().unwrap();
        for s in &stats {
            if s.name == table_name {
                return s.columns[0].count();
            }
        }
        0
    }

    /// Load the specified rows of line protocol data into this partition
    async fn load_data(partition: &mut Partition, lp_data: &[&str]) {
        let lp_string = lp_data.to_vec().join("\n");

        let lines: Vec<_> = parse_lines(&lp_string).map(|l| l.unwrap()).collect();
        let data = split_lines_into_write_entry_partitions(|_| partition.key().into(), &lines);

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&data);

        let entries = batch.entries().unwrap();
        for entry in entries {
            let key = entry
                .partition_key()
                .expect("partition key should have been inserted");
            assert_eq!(key, partition.key());

            partition.write_entry(&entry).unwrap()
        }
    }

    fn dump_table(partition: &Partition, table_name: &str) -> Vec<RecordBatch> {
        let mut dst = vec![];
        let requested_columns = []; // empty ==> request all columns
        partition
            .table_to_arrow(&mut dst, table_name, &requested_columns)
            .unwrap();

        // Now, sort dest
        dst.into_iter().map(sort_record_batch).collect()
    }

    /// returns a list of all chunk ids in partition that are not empty
    fn all_ids_with_data(partition: &Partition) -> Vec<u64> {
        partition
            .iter()
            .filter_map(|c| if c.is_empty() { None } else { Some(c.id()) })
            .collect()
    }
}
