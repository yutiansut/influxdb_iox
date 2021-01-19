//! This module contains code for managing the WAL buffer

use data_types::{
    data::ReplicatedWrite,
    database_rules::{WalBufferRollover, WriterId},
};
use generated_types::wal;
use object_store::path::ObjectStorePath;

use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    mem,
    sync::{Arc, Mutex},
};

//use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use crc32fast::Hasher;
use data_types::database_rules::WalBufferConfig;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use tracing::warn;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Max size limit hit {}", size))]
    MaxSizeLimit { size: u64 },

    #[snafu(display(
        "Unable to drop segment to reduce size below max. Current size {}, segment count {}",
        size,
        segment_count
    ))]
    UnableToDropSegment { size: u64, segment_count: usize },

    #[snafu(display(
        "Sequence from writer {} out of order. Current: {}, incomming {}",
        writer,
        current_sequence,
        incoming_sequence,
    ))]
    SequenceOutOfOrder {
        writer: WriterId,
        current_sequence: u64,
        incoming_sequence: u64,
    },

    #[snafu(display("segment id must be between [1, 1,000,000,000)"))]
    SegmentIdOutOfBounds,

    #[snafu(display("unable to compress segment id {}: {}", segment_id, source))]
    UnableToCompressData {
        segment_id: u64,
        source: snap::Error,
    },

    #[snafu(display("unable to decompress segment data: {}", source))]
    UnableToDecompressData { source: snap::Error },

    #[snafu(display("unable to read checksum: {}", source))]
    UnableToReadChecksum {
        source: std::array::TryFromSliceError,
    },

    #[snafu(display("checksum mismatch for segment"))]
    ChecksumMismatch,

    #[snafu(display("the flatbuffers Segment is invalid"))]
    InvalidFlatbuffersSegment,
}

#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An in-memory buffer of a write ahead log. It is split up into segments,
/// which can be persisted to object storage.
#[derive(Debug)]
pub struct Buffer {
    max_size: u64,
    current_size: u64,
    segment_size: u64,
    pub persist: bool,
    open_segment: Segment,
    closed_segments: Vec<Arc<Segment>>,
    rollover_behavior: WalBufferRollover,
}

impl Buffer {
    #[allow(dead_code)]
    pub fn new(
        max_size: u64,
        segment_size: u64,
        rollover_behavior: WalBufferRollover,
        persist: bool,
    ) -> Self {
        Self {
            max_size,
            segment_size,
            persist,
            rollover_behavior,
            open_segment: Segment::new(1),
            current_size: 0,
            closed_segments: vec![],
        }
    }

    /// Appends a replicated write onto the buffer, returning the segment if it
    /// has been closed out. If the max size of the buffer would be exceeded
    /// by accepting the write, the oldest (first) of the closed segments
    /// will be dropped, if it is persisted. Otherwise, an error is returned.
    #[allow(dead_code)]
    pub fn append(&mut self, write: Arc<ReplicatedWrite>) -> Result<Option<Arc<Segment>>> {
        let write_size = u64::try_from(write.data.len())
            .expect("appended data must be less than a u64 in length");

        while self.current_size + write_size > self.max_size {
            let oldest_is_persisted = match self.closed_segments.get(0) {
                Some(s) => s.persisted_at().is_some(),
                None => false,
            };

            if oldest_is_persisted {
                self.remove_oldest_segment();
                continue;
            }

            match self.rollover_behavior {
                WalBufferRollover::DropIncoming => {
                    warn!(
                        "WAL is full, dropping incoming write for current segment (segment id: {:?})",
                        self.open_segment.id,
                    );
                    return Ok(None);
                }
                WalBufferRollover::DropOldSegment => {
                    let oldest_segment_id = self.remove_oldest_segment();
                    warn!(
                        "WAL is full, dropping oldest segment (segment id: {:?})",
                        oldest_segment_id
                    );
                }
                WalBufferRollover::ReturnError => {
                    return UnableToDropSegment {
                        size: self.current_size,
                        segment_count: self.closed_segments.len(),
                    }
                    .fail()
                }
            }
        }

        let mut closed_segment = None;

        self.current_size += write_size;
        self.open_segment.append(write)?;
        if self.open_segment.size > self.segment_size {
            let next_id = self.open_segment.id + 1;
            let segment = mem::replace(&mut self.open_segment, Segment::new(next_id));
            let segment = Arc::new(segment);

            self.closed_segments.push(segment.clone());
            closed_segment = Some(segment);
        }

        Ok(closed_segment)
    }

    /// Returns the current size of the buffer.
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Returns any replicated writes from the given writer ID and sequence
    /// number onward. This will include writes from other writers. The
    /// given writer ID and sequence are to identify from what point to
    /// replay writes. If no write matches the given writer ID and sequence
    /// number, all replicated writes within the buffer will be returned.
    #[allow(dead_code)]
    pub fn all_writes_since(&self, since: WriterSequence) -> Vec<Arc<ReplicatedWrite>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for w in self.open_segment.writes.iter().rev() {
            if w.equal_to_writer_and_sequence(since.id, since.sequence) {
                writes.reverse();
                return writes;
            }
            writes.push(w.clone());
        }

        for s in self.closed_segments.iter().rev() {
            for w in s.writes.iter().rev() {
                if w.equal_to_writer_and_sequence(since.id, since.sequence) {
                    writes.reverse();
                    return writes;
                }
                writes.push(w.clone());
            }
        }

        writes.reverse();
        writes
    }

    /// Returns replicated writes from the given writer ID and sequence number
    /// onward. This returns only writes from the passed in writer ID. If no
    /// write matches the given writer ID and sequence number, all
    /// replicated writes within the buffer for that writer will be returned.
    #[allow(dead_code)]
    pub fn writes_since(&self, since: WriterSequence) -> Vec<Arc<ReplicatedWrite>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for w in self.open_segment.writes.iter().rev() {
            let (writer, sequence) = w.writer_and_sequence();
            if writer == since.id {
                if sequence == since.sequence {
                    writes.reverse();
                    return writes;
                }
                writes.push(w.clone());
            }
        }

        for s in self.closed_segments.iter().rev() {
            for w in s.writes.iter().rev() {
                let (writer, sequence) = w.writer_and_sequence();
                if writer == since.id {
                    if sequence == since.sequence {
                        writes.reverse();
                        return writes;
                    }
                    writes.push(w.clone());
                }
            }
        }

        writes.reverse();
        writes
    }

    // Removes the oldest segment present in the buffer, returning its id
    #[allow(dead_code)]
    fn remove_oldest_segment(&mut self) -> u64 {
        let removed_segment = self.closed_segments.remove(0);
        self.current_size -= removed_segment.size;
        removed_segment.id
    }
}

impl From<&WalBufferConfig> for Buffer {
    fn from(config: &WalBufferConfig) -> Self {
        Self::new(
            config.buffer_size,
            config.segment_size,
            config.buffer_rollover,
            config.store_segments,
        )
    }
}

/// Segment is a collection of replicated writes that can be persisted to
/// object store.
#[derive(Debug)]
pub struct Segment {
    pub(crate) id: u64,
    size: u64,
    pub writes: Vec<Arc<ReplicatedWrite>>,
    writers: BTreeMap<WriterId, WriterSummary>,
    // If set, this is the time at which this segment was persisted
    persisted: Mutex<Option<DateTime<Utc>>>,
}

impl Segment {
    #[allow(dead_code)]
    fn new(id: u64) -> Self {
        Self {
            id,
            size: 0,
            writes: vec![],
            writers: BTreeMap::new(),
            persisted: Mutex::new(None),
        }
    }

    fn new_with_capacity(id: u64, capacity: usize) -> Self {
        Self {
            id,
            size: 0,
            writes: Vec::with_capacity(capacity),
            writers: BTreeMap::new(),
            persisted: Mutex::new(None),
        }
    }

    // appends the write to the segment, keeping track of the summary information
    // about the writer
    #[allow(dead_code)]
    fn append(&mut self, write: Arc<ReplicatedWrite>) -> Result<()> {
        let (writer_id, sequence_number) = write.writer_and_sequence();
        self.validate_and_update_sequence_summary(writer_id, sequence_number)?;

        let size = write.data.len();
        let size = u64::try_from(size).expect("appended data must be less than a u64 in length");
        self.size += size;

        self.writes.push(write);
        Ok(())
    }

    // checks that the sequence numbers in this segment are monotonically
    // increasing. Also keeps track of the starting and ending sequence numbers
    // and if any were missing.
    fn validate_and_update_sequence_summary(
        &mut self,
        writer_id: WriterId,
        sequence_number: u64,
    ) -> Result<()> {
        match self.writers.get_mut(&writer_id) {
            Some(summary) => {
                if summary.end_sequence >= sequence_number {
                    return SequenceOutOfOrder {
                        writer: writer_id,
                        current_sequence: summary.end_sequence,
                        incoming_sequence: sequence_number,
                    }
                    .fail();
                } else if summary.end_sequence + 1 != sequence_number {
                    summary.missing_sequence = true;
                }

                summary.end_sequence = sequence_number;
            }
            None => {
                let summary = WriterSummary {
                    start_sequence: sequence_number,
                    end_sequence: sequence_number,
                    missing_sequence: false,
                };

                self.writers.insert(writer_id, summary);
            }
        }

        Ok(())
    }

    /// sets the time this segment was persisted at
    #[allow(dead_code)]
    pub fn set_persisted_at(&self, time: DateTime<Utc>) {
        let mut persisted = self.persisted.lock().expect("mutex poisoned");
        *persisted = Some(time);
    }

    /// returns the time this segment was persisted at or none if not set
    pub fn persisted_at(&self) -> Option<DateTime<Utc>> {
        let persisted = self.persisted.lock().expect("mutex poisoned");
        *persisted
    }

    // converts the segment to its flatbuffer bytes
    fn fb_bytes(&self, writer_id: u32) -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new_with_capacity(
            usize::try_from(self.size).expect("unable to serialize segment of this size"),
        );
        let writes = self
            .writes
            .iter()
            .map(|rw| {
                let payload = fbb.create_vector_direct(&rw.data);
                wal::ReplicatedWriteData::create(
                    &mut fbb,
                    &wal::ReplicatedWriteDataArgs {
                        payload: Some(payload),
                    },
                )
            })
            .collect::<Vec<flatbuffers::WIPOffset<wal::ReplicatedWriteData<'_>>>>();
        let writes = fbb.create_vector(&writes);

        let segment = wal::Segment::create(
            &mut fbb,
            &wal::SegmentArgs {
                id: self.id,
                writer_id,
                writes: Some(writes),
            },
        );

        fbb.finish(segment, None);

        let (mut data, idx) = fbb.collapse();
        data.split_off(idx)
    }

    /// serialize the segment to the bytes to represent it in a file. This
    /// compresses the flatbuffers payload and writes a crc32 checksum at
    /// the end.
    pub fn to_file_bytes(&self, writer_id: u32) -> Result<Bytes> {
        let fb_bytes = self.fb_bytes(writer_id);

        let mut encoder = snap::raw::Encoder::new();
        let mut compressed_data =
            encoder
                .compress_vec(&fb_bytes)
                .context(UnableToCompressData {
                    segment_id: self.id,
                })?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let checksum = hasher.finalize();

        compressed_data.extend_from_slice(&checksum.to_le_bytes());

        Ok(Bytes::from(compressed_data))
    }

    /// checks the crc32 for the compressed data, decompresses it and
    /// deserializes it into a Segment struct.
    pub fn from_file_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<u32>() {
            return Err(Error::InvalidFlatbuffersSegment);
        }

        let (data, checksum) = data.split_at(data.len() - std::mem::size_of::<u32>());
        let checksum = u32::from_le_bytes(checksum.try_into().context(UnableToReadChecksum)?);

        let mut hasher = Hasher::new();
        hasher.update(&data);

        if checksum != hasher.finalize() {
            return Err(Error::ChecksumMismatch);
        }

        let mut decoder = snap::raw::Decoder::new();
        let data = decoder
            .decompress_vec(data)
            .context(UnableToDecompressData)?;

        let fb_segment = flatbuffers::get_root::<wal::Segment<'_>>(&data);

        let writes = fb_segment.writes().context(InvalidFlatbuffersSegment)?;
        let mut segment = Self::new_with_capacity(fb_segment.id(), writes.len());
        for w in writes {
            let data = w.payload().context(InvalidFlatbuffersSegment)?;
            let rw = ReplicatedWrite {
                data: data.to_vec(),
            };
            segment.append(Arc::new(rw))?;
        }

        Ok(segment)
    }
}

/// The summary information for a writer that has data in a segment
#[derive(Debug, Eq, PartialEq)]
pub struct WriterSummary {
    start_sequence: u64,
    end_sequence: u64,
    missing_sequence: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct WriterSequence {
    pub id: WriterId,
    pub sequence: u64,
}

const WAL_DIR: &str = "wal";
const MAX_SEGMENT_ID: u64 = 999_999_999;
const SEGMENT_FILE_EXTENSION: &str = ".segment";

/// Builds the path for a given segment id, given the root object store path.
/// The path should be where the root of the database is (e.g. 1/my_db/).
pub fn object_store_path_for_segment(
    root_path: &ObjectStorePath,
    segment_id: u64,
) -> Result<ObjectStorePath> {
    ensure!(
        segment_id < MAX_SEGMENT_ID && segment_id > 0,
        SegmentIdOutOfBounds
    );

    let millions_place = segment_id / 1_000_000;
    let millions = millions_place * 1_000_000;
    let thousands_place = (segment_id - millions) / 1_000;
    let thousands = thousands_place * 1_000;
    let hundreds_place = segment_id - millions - thousands;

    let mut path = root_path.clone();
    path.push_all_dirs(&[
        WAL_DIR,
        &format!("{:03}", millions_place),
        &format!("{:03}", thousands_place),
    ]);
    path.set_file_name(format!("{:03}{}", hundreds_place, SEGMENT_FILE_EXTENSION));

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{data::lines_to_replicated_write, database_rules::DatabaseRules};
    use influxdb_line_protocol::parse_lines;
    use object_store::path::cloud::CloudConverter;

    #[test]
    fn append_increments_current_size_and_uses_existing_segment() {
        let max = 1 << 32;
        let segment = 1 << 16;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let size = write.data.len() as u64;
        assert_eq!(0, buf.size());
        let segment = buf.append(write).unwrap();
        assert_eq!(size, buf.size());
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert_eq!(size * 2, buf.size());
        assert!(segment.is_none());
    }

    #[test]
    fn append_rolls_over_segment() {
        let max = 1 << 16;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 1);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 2);
    }

    #[test]
    fn drops_persisted_segment_when_over_size() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);
        assert!(segment.persisted_at().is_none());
        segment.set_persisted_at(Utc::now());

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);
        assert!(segment.persisted_at().is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_old_segment_even_if_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::DropOldSegment, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_incoming_write_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::DropIncoming, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[test]
    fn returns_error_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        assert!(buf.append(write).is_err());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[test]
    fn all_writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let writes = buf.all_writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(5, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(2, 1));
        assert!(writes[2].equal_to_writer_and_sequence(1, 2));
        assert!(writes[3].equal_to_writer_and_sequence(1, 3));
        assert!(writes[4].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(4, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));
        assert!(writes[3].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));
        assert!(writes[2].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 3 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 2 });
        assert_eq!(0, writes.len());
    }

    #[test]
    fn writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let writes = buf.writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(0, writes.len());

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 0 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(2, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));
    }

    #[test]
    fn returns_error_if_sequence_decreases() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        assert!(buf.append(write).is_err());
    }

    #[test]
    fn segment_keeps_writer_summaries() {
        let mut segment = Segment::new(1);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 4, "cpu val=1 10");
        segment.append(write).unwrap();

        let summary = segment.writers.get(&1).unwrap();
        assert_eq!(
            &WriterSummary {
                start_sequence: 1,
                end_sequence: 2,
                missing_sequence: false
            },
            summary
        );

        let summary = segment.writers.get(&2).unwrap();
        assert_eq!(
            &WriterSummary {
                start_sequence: 1,
                end_sequence: 4,
                missing_sequence: true
            },
            summary
        );
    }

    #[test]
    fn object_store_path_for_segment() {
        let path = ObjectStorePath::from_cloud_unchecked("1/mydb");
        let segment_path = super::object_store_path_for_segment(&path, 23).unwrap();
        let segment_path = CloudConverter::convert(&segment_path);

        assert_eq!(segment_path, "1/mydb/wal/000/000/023.segment");

        let segment_path = super::object_store_path_for_segment(&path, 20_003).unwrap();
        let segment_path = CloudConverter::convert(&segment_path);

        assert_eq!(segment_path, "1/mydb/wal/000/020/003.segment");

        let segment_path = super::object_store_path_for_segment(&path, 45_010_105).unwrap();
        let segment_path = CloudConverter::convert(&segment_path);

        assert_eq!(segment_path, "1/mydb/wal/045/010/105.segment");
    }

    #[test]
    fn object_store_path_for_segment_out_of_bounds() {
        let path = ObjectStorePath::from_cloud_unchecked("1/mydb");
        let segment_path = super::object_store_path_for_segment(&path, 0)
            .err()
            .unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);

        let segment_path = super::object_store_path_for_segment(&path, 23_000_000_000)
            .err()
            .unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);
    }

    #[test]
    fn segment_serialize_deserialize() {
        let id = 1;
        let mut segment = Segment::new(id);
        let writer_id = 2;
        segment
            .append(lp_to_replicated_write(writer_id, 0, "foo val=1 123"))
            .unwrap();
        segment
            .append(lp_to_replicated_write(writer_id, 1, "foo val=2 124"))
            .unwrap();

        let data = segment.to_file_bytes(writer_id).unwrap();
        let recovered_segment = Segment::from_file_bytes(&data).unwrap();

        assert_eq!(segment.id, recovered_segment.id);
        assert_eq!(segment.size, recovered_segment.size);
        assert_eq!(segment.writes, recovered_segment.writes);
    }

    fn lp_to_replicated_write(
        writer_id: u32,
        sequence_number: u64,
        lp: &str,
    ) -> Arc<ReplicatedWrite> {
        let lines: Vec<_> = parse_lines(lp).map(|l| l.unwrap()).collect();
        let rules = DatabaseRules::default();
        Arc::new(lines_to_replicated_write(
            writer_id,
            sequence_number,
            &lines,
            &rules,
        ))
    }
}
