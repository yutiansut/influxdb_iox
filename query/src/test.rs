//! This module provides a reference implementaton of `query::DatabaseSource`
//! and `query::Database` for use in testing.

use arrow_deps::{
    datafusion::{logical_plan::LogicalPlan, physical_plan::SendableRecordBatchStream},
    util::str_iter_to_batch,
};

use crate::{exec::Executor, group_by::GroupByAndAggregate, util::make_scan_plan};
use crate::{
    exec::{
        stringset::{StringSet, StringSetRef},
        FieldListPlan, SeriesSetPlans, StringSetPlan,
    },
    Database, DatabaseStore, PartitionChunk, Predicate,
};

use data_types::{
    data::{lines_to_replicated_write, ReplicatedWrite},
    database_rules::{DatabaseRules, PartitionTemplate, TemplatePart},
    schema::Schema,
    selection::Selection,
};
use influxdb_line_protocol::{parse_lines, ParsedLine};

use async_trait::async_trait;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default)]
pub struct TestDatabase {
    /// Partitions which have been saved to this test database
    /// Key is partition name
    /// Value is map of chunk_id to chunk
    partitions: Mutex<BTreeMap<String, BTreeMap<u32, Arc<TestChunk>>>>,

    /// Lines which have been written to this database, in order
    saved_lines: Mutex<Vec<String>>,

    /// Replicated writes which have been written to this database, in order
    replicated_writes: Mutex<Vec<ReplicatedWrite>>,

    /// `column_names` to return upon next request
    column_names: Arc<Mutex<Option<StringSetRef>>>,

    /// The last request for `column_names`
    column_names_request: Arc<Mutex<Option<ColumnNamesRequest>>>,

    /// `column_values` to return upon next request
    column_values: Arc<Mutex<Option<StringSetRef>>>,

    /// The last request for `column_values`
    column_values_request: Arc<Mutex<Option<ColumnValuesRequest>>>,

    /// Responses to return on the next request to `query_series`
    query_series_values: Arc<Mutex<Option<SeriesSetPlans>>>,

    /// The last request for `query_series`
    query_series_request: Arc<Mutex<Option<QuerySeriesRequest>>>,

    /// Responses to return on the next request to `query_groups`
    query_groups_values: Arc<Mutex<Option<SeriesSetPlans>>>,

    /// The last request for `query_series`
    query_groups_request: Arc<Mutex<Option<QueryGroupsRequest>>>,

    /// Responses to return on the next request to `field_column_values`
    field_columns_value: Arc<Mutex<Option<FieldListPlan>>>,

    /// The last request for `query_series`
    field_columns_request: Arc<Mutex<Option<FieldColumnsRequest>>>,
}

/// Records the parameters passed to a column name request
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnNamesRequest {
    /// Stringified '{:?}' version of the predicate
    pub predicate: String,
}

/// Records the parameters passed to a column values request
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnValuesRequest {
    /// The name of the requested column
    pub column_name: String,

    /// Stringified '{:?}' version of the predicate
    pub predicate: String,
}

/// Records the parameters passed to a `query_series` request
#[derive(Debug, PartialEq, Clone)]
pub struct QuerySeriesRequest {
    /// Stringified '{:?}' version of the predicate
    pub predicate: String,
}

/// Records the parameters passed to a `query_groups` request
#[derive(Debug, PartialEq, Clone)]
pub struct QueryGroupsRequest {
    /// Stringified '{:?}' version of the predicate
    pub predicate: String,
    /// The requested aggregate
    pub gby_agg: GroupByAndAggregate,
}

/// Records the parameters passed to a `field_columns` request
#[derive(Debug, PartialEq, Clone)]
pub struct FieldColumnsRequest {
    /// Stringified '{:?}' version of the predicate
    pub predicate: String,
}

#[derive(Snafu, Debug)]
pub enum TestError {
    #[snafu(display("Test database error:  {}", message))]
    General { message: String },

    #[snafu(display("Test database execution:  {:?}", source))]
    Execution { source: crate::exec::Error },

    #[snafu(display("Test error writing to database: {}", source))]
    DatabaseWrite {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = TestError> = std::result::Result<T, E>;

impl TestDatabase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all lines written to this database
    pub fn get_lines(&self) -> Vec<String> {
        self.saved_lines.lock().expect("mutex poisoned").clone()
    }

    /// Get all replicated writs to this database
    pub fn get_writes(&self) -> Vec<ReplicatedWrite> {
        self.replicated_writes
            .lock()
            .expect("mutex poisoned")
            .clone()
    }

    /// Parse line protocol and add it as new lines to this
    /// database
    pub async fn add_lp_string(&self, lp_data: &str) {
        let parsed_lines = parse_lines(&lp_data)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| panic!("parsing line protocol: {}", lp_data));

        let mut writer = TestLPWriter::default();
        writer.write_lines(self, &parsed_lines).await.unwrap();

        // Writes parsed lines into this database
        let mut saved_lines = self.saved_lines.lock().expect("mutex poisoned");
        for line in parsed_lines {
            saved_lines.push(line.to_string())
        }
    }

    /// Add a test chunk to the database
    pub fn add_chunk(&self, partition_key: &str, chunk: Arc<TestChunk>) {
        let mut partitions = self.partitions.lock().expect("mutex poisoned");
        let chunks = partitions
            .entry(partition_key.to_string())
            .or_insert_with(BTreeMap::new);
        chunks.insert(chunk.id(), chunk);
    }

    /// Get the specified chunk
    pub fn get_chunk(&self, partition_key: &str, id: u32) -> Option<Arc<TestChunk>> {
        self.partitions
            .lock()
            .expect("mutex poisoned")
            .get(partition_key)
            .and_then(|p| p.get(&id).cloned())
    }

    /// Set the list of column names that will be returned on a call to
    /// column_names
    pub fn set_column_names(&self, column_names: Vec<String>) {
        let column_names = column_names.into_iter().collect::<StringSet>();
        let column_names = Arc::new(column_names);

        *(self.column_names.clone().lock().expect("mutex poisoned")) = Some(column_names)
    }

    /// Get the parameters from the last column name request
    pub fn get_column_names_request(&self) -> Option<ColumnNamesRequest> {
        self.column_names_request
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
    }

    /// Set the list of column values that will be returned on a call to
    /// column_values
    pub fn set_column_values(&self, column_values: Vec<String>) {
        let column_values = column_values.into_iter().collect::<StringSet>();
        let column_values = Arc::new(column_values);

        *(self.column_values.clone().lock().expect("mutex poisoned")) = Some(column_values)
    }

    /// Get the parameters from the last column name request
    pub fn get_column_values_request(&self) -> Option<ColumnValuesRequest> {
        self.column_values_request
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
    }

    /// Set the series that will be returned on a call to query_series
    pub fn set_query_series_values(&self, plan: SeriesSetPlans) {
        *(self
            .query_series_values
            .clone()
            .lock()
            .expect("mutex poisoned")) = Some(plan);
    }

    /// Get the parameters from the last column name request
    pub fn get_query_series_request(&self) -> Option<QuerySeriesRequest> {
        self.query_series_request
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
    }

    /// Set the series that will be returned on a call to query_groups
    pub fn set_query_groups_values(&self, plan: SeriesSetPlans) {
        *(self
            .query_groups_values
            .clone()
            .lock()
            .expect("mutex poisoned")) = Some(plan);
    }

    /// Get the parameters from the last column name request
    pub fn get_query_groups_request(&self) -> Option<QueryGroupsRequest> {
        self.query_groups_request
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
    }

    /// Set the FieldSet plan that will be returned
    pub fn set_field_colum_names_values(&self, plan: FieldListPlan) {
        *(self
            .field_columns_value
            .clone()
            .lock()
            .expect("mutex poisoned")) = Some(plan);
    }

    /// Get the parameters from the last column name request
    pub fn get_field_columns_request(&self) -> Option<FieldColumnsRequest> {
        self.field_columns_request
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
    }
}

/// returns true if this line is within the range of the timestamp
fn set_to_string(s: &BTreeSet<String>) -> String {
    s.iter().cloned().collect::<Vec<_>>().join(", ")
}

/// Convert a Predicate instance to a String that is reasonable to
/// compare directly in tests
fn predicate_to_test_string(predicate: &Predicate) -> String {
    let Predicate {
        table_names,
        field_columns,
        exprs,
        range,
        partition_key,
    } = predicate;

    let mut result = String::new();
    write!(result, "Predicate {{").unwrap();

    if let Some(table_names) = table_names {
        write!(result, " table_names: {}", set_to_string(table_names)).unwrap();
    }

    if let Some(field_columns) = field_columns {
        write!(result, " field_columns: {}", set_to_string(field_columns)).unwrap();
    }

    if !exprs.is_empty() {
        write!(result, " exprs: {:?}", exprs).unwrap();
    }

    if let Some(range) = range {
        write!(result, " range: {:?}", range).unwrap();
    }

    if let Some(partition_key) = partition_key {
        write!(result, " partition_key: {:?}", partition_key).unwrap();
    }

    write!(result, "}}").unwrap();
    result
}

#[async_trait]
impl Database for TestDatabase {
    type Error = TestError;
    type Chunk = TestChunk;

    /// Adds the replicated write to this database
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.replicated_writes
            .lock()
            .expect("mutex poisoned")
            .push(write.clone());
        Ok(())
    }

    /// Return the mocked out column names, recording the request
    async fn tag_column_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error> {
        // save the request
        let predicate = predicate_to_test_string(&predicate);

        let new_column_names_request = Some(ColumnNamesRequest { predicate });

        *self
            .column_names_request
            .clone()
            .lock()
            .expect("mutex poisoned") = new_column_names_request;

        // pull out the saved columns
        let column_names = self
            .column_names
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved column_names in TestDatabase",
            });

        Ok(column_names.into())
    }

    async fn field_column_names(&self, predicate: Predicate) -> Result<FieldListPlan, Self::Error> {
        // save the request
        let predicate = predicate_to_test_string(&predicate);

        let field_columns_request = Some(FieldColumnsRequest { predicate });

        *self
            .field_columns_request
            .clone()
            .lock()
            .expect("mutex poisoned") = field_columns_request;

        // pull out the saved columns
        self.field_columns_value
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved field_column_name in TestDatabase",
            })
    }

    /// Return the mocked out column values, recording the request
    async fn column_values(
        &self,
        column_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan, Self::Error> {
        // save the request
        let predicate = predicate_to_test_string(&predicate);

        let new_column_values_request = Some(ColumnValuesRequest {
            column_name: column_name.into(),
            predicate,
        });

        *self
            .column_values_request
            .clone()
            .lock()
            .expect("mutex poisoned") = new_column_values_request;

        // pull out the saved columns
        let column_values = self
            .column_values
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved column_values in TestDatabase",
            });

        Ok(column_values.into())
    }

    async fn query_series(&self, predicate: Predicate) -> Result<SeriesSetPlans, Self::Error> {
        let predicate = predicate_to_test_string(&predicate);

        let new_queries_series_request = Some(QuerySeriesRequest { predicate });

        *self
            .query_series_request
            .clone()
            .lock()
            .expect("mutex poisoned") = new_queries_series_request;

        self.query_series_values
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved query_series in TestDatabase",
            })
    }

    async fn query_groups(
        &self,
        predicate: Predicate,
        gby_agg: GroupByAndAggregate,
    ) -> Result<SeriesSetPlans, Self::Error> {
        let predicate = predicate_to_test_string(&predicate);

        let new_queries_groups_request = Some(QueryGroupsRequest { predicate, gby_agg });

        *self
            .query_groups_request
            .clone()
            .lock()
            .expect("mutex poisoned") = new_queries_groups_request;

        self.query_groups_values
            .clone()
            .lock()
            .expect("mutex poisoned")
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved query_groups in TestDatabase",
            })
    }

    /// Return the partition keys for data in this DB
    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        let partitions = self.partitions.lock().expect("mutex poisoned");
        let keys = partitions.keys().cloned().collect();
        Ok(keys)
    }

    async fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>> {
        let partitions = self.partitions.lock().expect("mutex poisoned");
        if let Some(chunks) = partitions.get(partition_key) {
            chunks.values().cloned().collect()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Default)]
pub struct TestChunk {
    pub id: u32,

    /// Table names to return back
    pub table_names: Vec<String>,

    /// A copy of the captured predicate passed
    pub table_names_predicate: std::sync::Mutex<Option<Predicate>>,
}

impl TestChunk {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn with_table(mut self, name: impl Into<String>) -> Self {
        self.table_names.push(name.into());
        self
    }

    /// Get a copy of any predicate passed to table_names
    pub fn table_names_predicate(&self) -> Option<Predicate> {
        self.table_names_predicate
            .lock()
            .expect("mutex poisoned")
            .as_ref()
            //.map(|v| v.clone())
            .cloned()
    }
}

#[async_trait]
impl PartitionChunk for TestChunk {
    type Error = TestError;

    fn id(&self) -> u32 {
        self.id
    }

    fn table_stats(
        &self,
    ) -> Result<Vec<data_types::partition_metadata::TableSummary>, Self::Error> {
        unimplemented!()
    }

    async fn read_filter(
        &self,
        _table_name: &str,
        _predicate: &Predicate,
        _selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        unimplemented!()
    }

    async fn table_names(&self, predicate: &Predicate) -> Result<LogicalPlan, Self::Error> {
        // save the predicate
        self.table_names_predicate
            .lock()
            .expect("mutex poisoned")
            .replace(predicate.clone());

        let names = self.table_names.iter().map(Some);
        let batch = str_iter_to_batch("tables", names).unwrap();
        Ok(make_scan_plan(batch).unwrap())
    }

    async fn table_schema(
        &self,
        _table_name: &str,
        _selection: Selection<'_>,
    ) -> Result<Schema, Self::Error> {
        unimplemented!()
    }

    async fn has_table(&self, _table_name: &str) -> bool {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse line protocol and add it as new lines to the `db_name` database
    pub async fn add_lp_string(&self, db_name: &str, lp_data: &str) {
        self.db_or_create(db_name)
            .await
            .expect("db_or_create suceeeds")
            .add_lp_string(lp_data)
            .await
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new()),
        }
    }
}

#[async_trait]
impl DatabaseStore for TestDatabaseStore {
    type Database = TestDatabase;
    type Error = TestError;

    /// List the database names.
    async fn db_names_sorted(&self) -> Vec<String> {
        let databases = self.databases.lock().expect("mutex poisoned");

        databases.keys().cloned().collect()
    }

    /// Retrieve the database specified name
    async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        let databases = self.databases.lock().expect("mutex poisoned");

        databases.get(name).cloned()
    }

    /// Retrieve the database specified by name, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let mut databases = self.databases.lock().expect("mutex poisoned");

        if let Some(db) = databases.get(name) {
            Ok(db.clone())
        } else {
            let new_db = Arc::new(TestDatabase::new());
            databases.insert(name.to_string(), new_db.clone());
            Ok(new_db)
        }
    }

    fn executor(&self) -> Arc<Executor> {
        self.executor.clone()
    }
}

/// Helper for writing line protocol data directly into test databases
/// (handles creating sequence numbers and writer ids
#[derive(Debug, Default)]
pub struct TestLPWriter {
    writer_id: u32,
    sequence_number: u64,
}

impl TestLPWriter {
    // writes data in LineProtocol format into a database
    pub async fn write_lines<D: Database>(
        &mut self,
        database: &D,
        lines: &[ParsedLine<'_>],
    ) -> Result<()> {
        // partitions data in hourly segments
        let partition_template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%dT%H".to_string())],
        };

        let rules = DatabaseRules {
            partition_template,
            ..Default::default()
        };

        let write = lines_to_replicated_write(self.writer_id, self.sequence_number, &lines, &rules);
        self.sequence_number += 1;
        database
            .store_replicated_write(&write)
            .await
            .map_err(|e| TestError::DatabaseWrite {
                source: Box::new(e),
            })
    }

    /// Writes line protocol formatted data in lp_data to `database`
    pub async fn write_lp_string<D: Database>(
        &mut self,
        database: &D,
        lp_data: &str,
    ) -> Result<()> {
        let lines = parse_lines(lp_data)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Box::new(e) as _)
            .context(DatabaseWrite)?;

        self.write_lines(database, &lines).await
    }
}
