/// This module contains code for snapshotting a database partition to Parquet files in object
/// storage.

use data_types::partition_metadata::{
    Partition as PartitionMeta,
    Table,
};
use object_store::ObjectStore;
use storage::Partition;
use arrow_deps::{
    arrow::record_batch::RecordBatch,
    parquet::{
        file::writer::TryClone,
        arrow::ArrowWriter,
    }
};

use std::sync::{Arc, Mutex};
use std::io::{Write, Seek, SeekFrom, Cursor};

use bytes::Bytes;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Partition error creating snapshot: {}", source))]
    PartitionError{ source: write_buffer::partition::Error },

    #[snafu(display("Table not found running: {}", table))]
    TableNotFound{ table: usize },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError{ source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Snapshot {
    pub id: Uuid,
    pub partition_meta: PartitionMeta,
    status: Mutex<Status>,
}

impl Snapshot {
    fn new(partition_key: String, tables: Vec<Table>) -> Self {
        let remaining_tables: Vec<_> = (0..tables.len()).collect();
        let status = Status{
            remaining_tables,
            ..Default::default()
        };

        Self{
            id: Uuid::new_v4(),
            partition_meta: PartitionMeta{
                key: partition_key,
                tables,
            },
            status: Mutex::new(status),
        }
    }

    fn next_table(&self) -> Option<(usize, &str)> {
        let mut status = self.status.lock().expect("mutex poisoned");
        status.next_table_position().map_or(None, |pos| {
            Some((pos, &self.partition_meta.tables[pos].name))
        })
    }

    fn mark_table_finished(&self, table_position: usize) -> Result<()> {
        let mut status = self.status.lock().expect("mutex poisoned");
        status.mark_table_finished(table_position)
    }

    fn mark_meta_written(&self) {
        let mut status = self.status.lock().expect("mutex poisoned");
        status.meta_written = true;
    }

    pub fn finished(&self) -> bool {
        self.status.lock().expect("mutex poisoned").finished()
    }
}

#[derive(Debug, Default)]
pub struct Status {
    remaining_tables: Vec<usize>,
    running_tables: Vec<usize>,
    finished_tables: Vec<usize>,
    meta_written: bool,
    stop_on_next_update: bool,
    errors: Vec<Error>,
}

impl Status {
    fn next_table_position(&mut self) -> Option<usize> {
        self.remaining_tables.pop().map_or(None, |t| {
            self.running_tables.push(t.clone());
            Some(t)
        })
    }

    fn mark_table_finished(&mut self, position: usize) -> Result<()> {
        let pos = self
            .running_tables
            .iter()
            .position(|t| *t == position)
            .context(TableNotFound{ table: position })?;

        let _ = self.running_tables.remove(pos);
        self.finished_tables.push(position);

        Ok(())
    }

    fn finished(&self) -> bool {
        self.remaining_tables.is_empty() && self.running_tables.is_empty() && self.meta_written
    }
}

pub fn snapshot_partition<T: Send + Sync + Partition>(
    metadata_path: impl Into<String>,
    data_path: impl Into<String>,
    store: Arc<ObjectStore>,
    partition: Arc<T>,
    notify: Option<oneshot::Sender<()>>,
) -> Result<Arc<Snapshot>> {
    let metadata_path = metadata_path.into();
    let data_path = data_path.into();

    let table_stats = partition.table_stats().unwrap();

    let snapshot = Snapshot::new(partition.key().to_string(), table_stats);
    let snapshot = Arc::new(snapshot);

    let return_snapshot = snapshot.clone();

    tokio::spawn(async move {
        while let Some((pos, table_name)) = snapshot.next_table() {
            let batch = partition.table_to_arrow(table_name, &[]).unwrap();

            let file_name = format!("{}/{}.parquet", &data_path, table_name);

            write_batch(batch, snapshot.clone(), &file_name, store.clone())
                .await
                .unwrap();

            snapshot.mark_table_finished(pos).unwrap();
        }

        let partition_meta_path = format!("{}/{}.json", &metadata_path, &partition.key());
        let json_data = serde_json::to_vec(&snapshot.partition_meta).context(JsonGenerationError).unwrap();
        let data = Bytes::from(json_data);
        let len = data.len();
        let stream_data = std::io::Result::Ok(data);
        store
            .put(
                &partition_meta_path,
                futures::stream::once(async move { stream_data }),
                len,
            )
            .await
            .unwrap();

        snapshot.mark_meta_written();

        if let Some(notify) = notify {
            // TODO: log this error only to the log
            let _ = notify.send(());
        }
    });

    Ok(return_snapshot)
}

async fn write_batch(batch: RecordBatch, snapshot: Arc<Snapshot>, file_name: &str, store: Arc<ObjectStore>) -> Result<()> {
    let mem_writer = MemWriter::default();
    {
        let mut writer =
            ArrowWriter::try_new(mem_writer.clone(), batch.schema().clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    } // drop the reference to the MemWriter that the SerializedFileWriter has

    let data = mem_writer
        .into_inner()
        .expect("Nothing else should have a reference here");

    let len = data.len();
    let data = Bytes::from(data);
    let stream_data = std::io::Result::Ok(data);

    store
        .put(
            &file_name,
            futures::stream::once(async move { stream_data }),
            len)
        .await
        .unwrap();

    Ok(())
}

#[derive(Debug, Default, Clone)]
struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .and_then(|mutex| mutex.into_inner().ok())
            .map(|cursor| cursor.into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock().unwrap();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock().unwrap();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: self.mem.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb_line_protocol::parse_lines;
    use data_types::data::lines_to_replicated_write;
    use data_types::database_rules::DatabaseRules;
    use object_store::InMemory;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn snapshot() {
        let lp = r#"
cpu,host=A,region=west user=23.2,system=55.1 1
cpu,host=A,region=west user=3.2,system=50.1 10
cpu,host=B,region=east user=10.0,system=74.1 1
mem,host=A,region=west used=45 1
        "#;

        let lines: Vec<_> = parse_lines(lp).map(|l| l.unwrap()).collect();
        let write = lines_to_replicated_write(1, 1, &lines, &DatabaseRules::default());
        let mut partition = Partition::new("testaroo");

        for e in write.write_buffer_batch().unwrap().entries().unwrap() {
            partition.write_entry(&e).unwrap();
        }

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let partition = Arc::new(partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metadata_path = "/meta";
        let data_path = "/data";

        let snapshot = snapshot_partition(
            metadata_path,
            data_path,
            partition.clone(),
            store.clone(),
            Some(tx)
        ).unwrap();

        rx.await.unwrap();

        let summary = store
            .get("/meta/testaroo.json")
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let meta: PartitionMeta = serde_json::from_slice(&*summary).unwrap();
        assert_eq!(meta, snapshot.partition_meta);
    }
}
