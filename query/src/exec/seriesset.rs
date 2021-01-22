//! This module contains the definition of a "SeriesSet" a plan that when run
//! produces rows that can be logically divided into "Series"
//!
//! Specifically, this thing can produce represents a set of "tables",
//! and each table is sorted on a set of "tag" columns, meaning the
//! data for groups / series will be contiguous.
//!
//! For example, the output columns of such a plan would be:
//! (tag col0) (tag col1) ... (tag colN) (field val1) (field val2) ... (field
//! valN) .. (timestamps)
//!
//! Note that the data will come out ordered by the tag keys (ORDER BY
//! (tag col0) (tag col1) ... (tag colN))
//!
//! NOTE: The InfluxDB classic storage engine not only returns
//! series sorted by the tag values, but the order of the tag columns
//! (and thus the actual sort order) is also lexographically
//! sorted. So for example, if you have `region`, `host`, and
//! `service` as tags, the columns would be ordered `host`, `region`,
//! and `service` as well.

use std::sync::Arc;

use arrow::{array::StringArray, datatypes::DataType, record_batch::RecordBatch};
use arrow_deps::{
    arrow::{self},
    datafusion::physical_plan::SendableRecordBatchStream,
};
use snafu::{ResultExt, Snafu};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{self, error::SendError};

use croaring::bitmap::Bitmap;

use super::field::{FieldColumns, FieldIndexes};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display(
        "Error reading record batch while converting from SeriesSet: {:?}",
        source
    ))]
    ReadingRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Internal field error while converting series set: {}", source))]
    InternalField { source: super::field::Error },

    #[snafu(display("Sending series set results during conversion: {:?}", source))]
    SendingDuringConversion {
        source: Box<SendError<Result<SeriesSetItem>>>,
    },

    #[snafu(display("Sending grouped series set results during conversion: {:?}", source))]
    SendingDuringGroupedConversion {
        source: Box<SendError<Result<SeriesSetItem>>>,
    },

    #[snafu(display("Joining conversion execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
/// Represents several logical timeseries that share the same
/// timestamps and name=value tag keys.
///
/// The heavy use of `Arc` is to avoid many duplicated Strings given
/// the the fact that many SeriesSets share the same tag keys and
/// table name.
pub struct SeriesSet {
    /// The table name this series came from
    pub table_name: Arc<String>,

    /// key = value pairs that define this series
    pub tags: Vec<(Arc<String>, Arc<String>)>,

    /// the column index of each "field" of the time series. For
    /// example, if there are two field indexes then this series set
    /// would result in two distinct series being sent back, one for
    /// each field.
    pub field_indexes: FieldIndexes,

    // The row in the record batch where the data starts (inclusive)
    pub start_row: usize,

    // The number of rows in the record batch that the data goes to
    pub num_rows: usize,

    // The underlying record batch data
    pub batch: RecordBatch,
}

/// Describes a group of series "group of series" series. Namely,
/// several logical timeseries that share the same timestamps and
/// name=value tag keys, grouped by some subset of the tag keys
///
/// TODO: this may also support computing an aggregation per group,
/// pending on what is required for the gRPC layer.
#[derive(Debug)]
pub struct GroupDescription {
    /// key = value  pairs that define the group
    pub tags: Vec<(Arc<String>, Arc<String>)>,
}

#[derive(Debug)]
pub enum SeriesSetItem {
    GroupStart(GroupDescription),
    Data(SeriesSet),
}

// Handles converting record batches into SeriesSets, and sending them
// to tx
#[derive(Debug)]
pub struct SeriesSetConverter {
    tx: mpsc::Sender<Result<SeriesSetItem>>,
}

impl SeriesSetConverter {
    pub fn new(tx: mpsc::Sender<Result<SeriesSetItem>>) -> Self {
        Self { tx }
    }

    /// Convert the results from running a DataFusion plan into the
    /// appropriate SeriesSetItems, sending them self.tx
    ///
    /// The results must be in the logical format described in this
    /// module's documentation (i.e. ordered by tag keys)
    ///
    /// table_name: The name of the table
    ///
    /// tag_columns: The names of the columns that define tags
    ///
    /// field_columns: The names of the columns which are "fields"
    ///
    /// num_prefix_tag_group_columns: (optional) The size of the prefix
    ///       of `tag_columns` that defines each group
    ///
    /// it: record batch iterator that produces data in the desired order
    pub async fn convert(
        &mut self,
        table_name: Arc<String>,
        tag_columns: Arc<Vec<Arc<String>>>,
        field_columns: FieldColumns,
        num_prefix_tag_group_columns: Option<usize>,
        it: SendableRecordBatchStream,
    ) -> Result<()> {
        // Make sure that any error that results from processing is sent along
        if let Err(e) = self
            .convert_impl(
                table_name,
                tag_columns,
                field_columns,
                num_prefix_tag_group_columns,
                it,
            )
            .await
        {
            self.tx
                .send(Err(e))
                .await
                .map_err(|e| Error::SendingDuringConversion {
                    source: Box::new(e),
                })?
        }
        Ok(())
    }

    /// Does the actual conversion, returning any error in processing
    pub async fn convert_impl(
        &mut self,
        table_name: Arc<String>,
        tag_columns: Arc<Vec<Arc<String>>>,
        field_columns: FieldColumns,
        num_prefix_tag_group_columns: Option<usize>,
        mut it: SendableRecordBatchStream,
    ) -> Result<()> {
        let mut group_generator = GroupGenerator::new(num_prefix_tag_group_columns);

        // for now, only handle a single record batch
        if let Some(batch) = it.next().await {
            let batch = batch.context(ReadingRecordBatch)?;

            if it.next().await.is_some() {
                // but not yet
                unimplemented!("Computing series across multiple record batches not yet supported");
            }

            let schema = batch.schema();
            // TODO: check that the tag columns are sorted by tag name...
            let tag_indexes =
                FieldIndexes::names_to_indexes(&schema, &tag_columns).context(InternalField)?;
            let field_indexes =
                FieldIndexes::from_field_columns(&schema, &field_columns).context(InternalField)?;

            // Algorithm: compute, via bitsets, the rows at which each
            // tag column changes and thereby where the tagset
            // changes. Emit a new SeriesSet at each such transition
            let mut tag_transitions = tag_indexes
                .iter()
                .map(|&col| Self::compute_transitions(&batch, col))
                .collect::<Result<Vec<_>>>()?;

            // no tag columns, emit a single tagset
            let intersections = if tag_transitions.is_empty() {
                let mut b = Bitmap::create_with_capacity(1);
                let end_row = batch.num_rows();
                b.add(end_row as u32);
                b
            } else {
                // OR bitsets together to to find all rows where the
                // keyset (values of the tag keys) changes
                let remaining = tag_transitions.split_off(1);

                remaining
                    .into_iter()
                    .for_each(|b| tag_transitions[0].or_inplace(&b));
                // take the first item
                tag_transitions.into_iter().next().unwrap()
            };

            let mut start_row: u32 = 0;

            // create each series (since bitmap are not Send, we can't
            // call await during the loop)

            // emit each series
            let series_sets = intersections
                .iter()
                .map(|end_row| {
                    let series_set = SeriesSet {
                        table_name: table_name.clone(),
                        tags: Self::get_tag_keys(
                            &batch,
                            start_row as usize,
                            &tag_columns,
                            &tag_indexes,
                        ),
                        field_indexes: field_indexes.clone(),
                        start_row: start_row as usize,
                        num_rows: (end_row - start_row) as usize,
                        batch: batch.clone(),
                    };

                    start_row = end_row;
                    series_set
                })
                .collect::<Vec<_>>();

            for series_set in series_sets {
                if let Some(group_desc) = group_generator.next_series(&series_set) {
                    self.tx
                        .send(Ok(SeriesSetItem::GroupStart(group_desc)))
                        .await
                        .map_err(|e| Error::SendingDuringGroupedConversion {
                            source: Box::new(e),
                        })?;
                }

                self.tx
                    .send(Ok(SeriesSetItem::Data(series_set)))
                    .await
                    .map_err(|e| Error::SendingDuringConversion {
                        source: Box::new(e),
                    })?;
            }
        }
        Ok(())
    }

    /// returns a bitset with all row indexes where the value of the
    /// batch[col_idx] changes.  Does not include row 0, always includes
    /// the last row, `batch.num_rows() - 1`
    fn compute_transitions(batch: &RecordBatch, col_idx: usize) -> Result<Bitmap> {
        let num_rows = batch.num_rows();

        let mut bitmap = Bitmap::create_with_capacity(num_rows as u32);
        if num_rows < 1 {
            return Ok(bitmap);
        }

        // otherwise, scan the column for transitions
        let col = batch.column(col_idx);
        match col.data_type() {
            DataType::Utf8 => {
                let col = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Casting column");
                let mut current_val = col.value(0);
                for row in 1..num_rows {
                    let next_val = col.value(row);
                    if next_val != current_val {
                        bitmap.add(row as u32);
                        current_val = next_val;
                    }
                }
            }
            _ => unimplemented!(
                "Series transition calculations not supported for type {:?} in column {:?}",
                col.data_type(),
                batch.schema().fields()[col_idx]
            ),
        }

        // for now, always treat the last row as ending a series
        bitmap.add(num_rows as u32);

        Ok(bitmap)
    }

    /// Creates (column_name, column_value) pairs for each column
    /// named in `tag_column_name` at the corresponding index
    /// `tag_indexes`
    fn get_tag_keys(
        batch: &RecordBatch,
        row: usize,
        tag_column_names: &[Arc<String>],
        tag_indexes: &[usize],
    ) -> Vec<(Arc<String>, Arc<String>)> {
        assert_eq!(tag_column_names.len(), tag_indexes.len());

        tag_column_names
            .iter()
            .zip(tag_indexes)
            .map(|(column_name, column_index)| {
                let tag_value: String = batch
                    .column(*column_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Tag column was a String")
                    .value(row)
                    .into();
                (column_name.clone(), Arc::new(tag_value))
            })
            .collect()
    }
}

/// Encapsulates the logic to generate new GroupFrames
struct GroupGenerator {
    num_prefix_tag_group_columns: Option<usize>,

    // vec of num_prefix_tag_group_columns, if any
    last_group_tags: Option<Vec<(Arc<String>, Arc<String>)>>,
}

impl GroupGenerator {
    fn new(num_prefix_tag_group_columns: Option<usize>) -> Self {
        Self {
            num_prefix_tag_group_columns,
            last_group_tags: None,
        }
    }

    /// Process the next SeriesSet in the sequence. Return
    /// `Some(GroupDescription{..})` if this marks the start of a new
    /// group. Return None otherwise
    fn next_series(&mut self, series_set: &SeriesSet) -> Option<GroupDescription> {
        if let Some(num_prefix_tag_group_columns) = self.num_prefix_tag_group_columns {
            // figure out if we are in a new group
            let need_group_start = match &self.last_group_tags {
                None => true,
                Some(last_group_tags) => {
                    last_group_tags.as_slice() != &series_set.tags[0..num_prefix_tag_group_columns]
                }
            };

            if need_group_start {
                let group_tags = series_set.tags[0..num_prefix_tag_group_columns].to_vec();

                let group_desc = GroupDescription {
                    tags: group_tags.clone(),
                };

                self.last_group_tags = Some(group_tags);
                return Some(group_desc);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        csv,
        datatypes::DataType,
        datatypes::Field,
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use arrow_deps::datafusion::physical_plan::common::SizedRecordBatchStream;
    use test_helpers::{str_pair_vec_to_vec, str_vec_to_arc_vec};

    use super::*;

    #[tokio::test]
    async fn test_convert_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![]));
        let empty_iterator = Box::pin(SizedRecordBatchStream::new(schema, vec![]));

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = [];

        let results = convert(table_name, &tag_columns, &field_columns, empty_iterator).await;
        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags() -> Result<()> {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n",
        );

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = ["float_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 1);
        let series_set = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set.table_name, "foo");
        assert!(series_set.tags.is_empty());
        assert_eq!(
            series_set.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
        );
        assert_eq!(series_set.start_row, 0);
        assert_eq!(series_set.num_rows, 2);

        // Test that the record batch made it through
        let expected_data = vec![
            "+-------+-------+-------------+-----------+------+",
            "| tag_a | tag_b | float_field | int_field | time |",
            "+-------+-------+-------------+-----------+------+",
            "| one   | ten   | 10          | 1         | 1000 |",
            "| one   | ten   | 10.1        | 2         | 2000 |",
            "+-------+-------+-------------+-----------+------+",
            "",
        ];

        let actual_data = pretty_format_batches(&[series_set.batch.clone()])
            .expect("formatting batch")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        assert_eq!(expected_data, actual_data);
        Ok(())
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags_nulls() -> Result<()> {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        // send no values in the int_field colum
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,,1000\n\
             one,ten,10.1,,2000\n",
        );

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = ["float_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 1);
        let series_set = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set.table_name, "foo");
        assert!(series_set.tags.is_empty());
        assert_eq!(
            series_set.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
        );
        assert_eq!(series_set.start_row, 0);
        assert_eq!(series_set.num_rows, 2);

        // Test that the record batch made it through
        let expected_data = vec![
            "+-------+-------+-------------+-----------+------+",
            "| tag_a | tag_b | float_field | int_field | time |",
            "+-------+-------+-------------+-----------+------+",
            "| one   | ten   | 10          |           | 1000 |",
            "| one   | ten   | 10.1        |           | 2000 |",
            "+-------+-------+-------------+-----------+------+",
            "",
        ];

        let actual_data = pretty_format_batches(&[series_set.batch.clone()])
            .expect("formatting batch")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        assert_eq!(expected_data, actual_data);
        Ok(())
    }

    #[tokio::test]
    async fn test_convert_single_series_one_tag() -> Result<()> {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n",
        );

        // test with one tag column, one series
        let table_name = "bar";
        let tag_columns = ["tag_a"];
        let field_columns = ["float_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 1);
        let series_set = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set.table_name, "bar");
        assert_eq!(series_set.tags, str_pair_vec_to_vec(&[("tag_a", "one")]));
        assert_eq!(
            series_set.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
        );
        assert_eq!(series_set.start_row, 0);
        assert_eq!(series_set.num_rows, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_one_tag_multi_series() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n\
             one,eleven,10.1,3,3000\n\
             two,eleven,10.2,4,4000\n\
             two,eleven,10.3,5,5000\n",
        );

        let table_name = "foo";
        let tag_columns = ["tag_a"];
        let field_columns = ["int_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 2);
        let series_set1 = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(series_set1.tags, str_pair_vec_to_vec(&[("tag_a", "one")]));
        assert_eq!(
            series_set1.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[3])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 3);

        let series_set2 = results[1].as_ref().expect("Correctly converted");

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(series_set2.tags, str_pair_vec_to_vec(&[("tag_a", "two")]));
        assert_eq!(
            series_set2.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[3])
        );
        assert_eq!(series_set2.start_row, 3);
        assert_eq!(series_set2.num_rows, 2);

        Ok(())
    }

    // two tag columns, three series
    #[tokio::test]
    async fn test_convert_two_tag_multi_series() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n\
             one,eleven,10.1,3,3000\n\
             two,eleven,10.2,4,4000\n\
             two,eleven,10.3,5,5000\n",
        );

        let table_name = "foo";
        let tag_columns = ["tag_a", "tag_b"];
        let field_columns = ["int_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 3);
        let series_set1 = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 2);

        let series_set2 = results[1].as_ref().expect("Correctly converted");

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set2.start_row, 2);
        assert_eq!(series_set2.num_rows, 1);

        let series_set3 = results[2].as_ref().expect("Correctly converted");

        assert_eq!(*series_set3.table_name, "foo");
        assert_eq!(
            series_set3.tags,
            str_pair_vec_to_vec(&[("tag_a", "two"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set3.start_row, 3);
        assert_eq!(series_set3.num_rows, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_groups() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1000\n\
             one,eleven,10.1,2000\n\
             two,eleven,10.3,3000\n",
        );

        let table_name = "foo";
        let tag_columns = ["tag_a", "tag_b"];
        let num_prefix_tag_group_columns = 1;
        let field_columns = ["float_field"];
        let results = convert_groups(
            table_name,
            &tag_columns,
            num_prefix_tag_group_columns,
            &field_columns,
            input,
        )
        .await;

        // expect the output to be
        // Group1 (tag_a = one)
        // Series1 (tag_a = one, tag_b = ten)
        // Series2 (tag_a = one, tag_b = ten)
        // Group2 (tag_a = two)
        // Series3 (tag_a = two, tag_b = eleven)

        assert_eq!(results.len(), 5, "results were\n{:#?}", results); // 3 series, two groups (one and two)

        let group_1 = extract_group(results[0].as_ref().expect("correctly made group"));
        let series_set1 = extract_series_set(results[1].as_ref().expect("Correctly converted"));
        let series_set2 = extract_series_set(results[2].as_ref().expect("Correctly converted"));
        let group_2 = extract_group(results[3].as_ref().expect("correctly made group"));
        let series_set3 = extract_series_set(results[4].as_ref().expect("Correctly converted"));

        assert_eq!(group_1.tags, str_pair_vec_to_vec(&[("tag_a", "one")]));

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 1);

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set2.start_row, 1);
        assert_eq!(series_set2.num_rows, 1);

        assert_eq!(group_2.tags, str_pair_vec_to_vec(&[("tag_a", "two")]));

        assert_eq!(*series_set3.table_name, "foo");
        assert_eq!(
            series_set3.tags,
            str_pair_vec_to_vec(&[("tag_a", "two"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set3.start_row, 2);
        assert_eq!(series_set3.num_rows, 1);

        Ok(())
    }

    // test with no group tags specified
    #[tokio::test]
    async fn test_convert_groups_no_tags() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1000\n\
             one,eleven,10.1,2000\n",
        );

        let table_name = "foo";
        let tag_columns = ["tag_a", "tag_b"];
        let field_columns = ["float_field"];
        let results = convert_groups(table_name, &tag_columns, 0, &field_columns, input).await;

        // expect the output to be
        // Group1 (tag_a = one)
        // Series1 (tag_a = one, tag_b = ten)
        // Series2 (tag_a = one, tag_b = ten)

        assert_eq!(results.len(), 3, "results were\n{:#?}", results); // 3 series, two groups (one and two)

        let group_1 = extract_group(results[0].as_ref().expect("correctly made group"));
        let series_set1 = extract_series_set(results[1].as_ref().expect("Correctly converted"));
        let series_set2 = extract_series_set(results[2].as_ref().expect("Correctly converted"));

        assert_eq!(group_1.tags, &[]);

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 1);

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set2.start_row, 1);
        assert_eq!(series_set2.num_rows, 1);

        Ok(())
    }

    fn extract_group(item: &SeriesSetItem) -> &GroupDescription {
        match item {
            SeriesSetItem::GroupStart(group) => group,
            _ => panic!("Expected group, but got: {:?}", item),
        }
    }

    fn extract_series_set(item: &SeriesSetItem) -> &SeriesSet {
        match item {
            SeriesSetItem::Data(series_set) => series_set,
            _ => panic!("Expected series set, but got: {:?}", item),
        }
    }

    /// Test helper: run conversion and return a Vec
    pub async fn convert<'a>(
        table_name: &'a str,
        tag_columns: &'a [&'a str],
        field_columns: &'a [&'a str],
        it: SendableRecordBatchStream,
    ) -> Vec<Result<SeriesSet>> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut converter = SeriesSetConverter::new(tx);

        let table_name = Arc::new(table_name.into());
        let tag_columns = str_vec_to_arc_vec(tag_columns);
        let field_columns = FieldColumns::from(field_columns);

        tokio::task::spawn(async move {
            converter
                .convert(table_name, tag_columns, field_columns, None, it)
                .await
                .expect("Conversion happened without error")
        });

        let mut results = Vec::new();
        while let Some(r) = rx.recv().await {
            results.push(r.map(|item| {
                if let SeriesSetItem::Data(series_set) = item {
                    series_set
                }
                else {
                    panic!("Unexpected result from converting. Expected SeriesSetItem::Data, got: {:?}", item)
                }
            })
            );
        }
        results
    }

    /// Test helper: run conversion to groups and return a Vec
    pub async fn convert_groups<'a>(
        table_name: &'a str,
        tag_columns: &'a [&'a str],
        num_prefix_tag_group_columns: usize,
        field_columns: &'a [&'a str],
        it: SendableRecordBatchStream,
    ) -> Vec<Result<SeriesSetItem>> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut converter = SeriesSetConverter::new(tx);

        let table_name = Arc::new(table_name.into());
        let tag_columns = str_vec_to_arc_vec(tag_columns);
        let field_columns = FieldColumns::from(field_columns);

        tokio::task::spawn(async move {
            converter
                .convert(
                    table_name,
                    tag_columns,
                    field_columns,
                    Some(num_prefix_tag_group_columns),
                    it,
                )
                .await
                .expect("Conversion happened without error")
        });

        let mut results = Vec::new();
        while let Some(r) = rx.recv().await {
            results.push(r)
        }
        results
    }

    /// Test helper: parses the csv content into a single record batch arrow
    /// arrays columnar ArrayRef according to the schema
    fn parse_to_record_batch(schema: SchemaRef, data: &str) -> RecordBatch {
        let has_header = false;
        let delimiter = Some(b',');
        let batch_size = 1000;
        let bounds = None;
        let projection = None;
        let mut reader = csv::Reader::new(
            data.as_bytes(),
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection,
        );

        let first_batch = reader.next().expect("Reading first batch");
        assert!(
            first_batch.is_ok(),
            "Can not parse record batch from csv: {:?}",
            first_batch
        );
        assert!(
            reader.next().is_none(),
            "Unexpected batch while parsing csv"
        );

        first_batch.unwrap()
    }

    fn parse_to_iterator(schema: SchemaRef, data: &str) -> SendableRecordBatchStream {
        let batch = parse_to_record_batch(schema.clone(), data);
        Box::pin(SizedRecordBatchStream::new(schema, vec![Arc::new(batch)]))
    }
}
