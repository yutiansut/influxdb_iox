use std::collections::{BTreeMap, HashMap};

use arrow_deps::arrow::datatypes::SchemaRef;

use fxhash::FxHashMap;

use crate::column::{
    cmp::Operator, AggregateResult, AggregateType, Column, EncodedValues, OwnedValue, RowIDs,
    RowIDsOption, Scalar, Value, Values, ValuesIterator,
};

/// The name used for a timestamp column.
pub const TIME_COLUMN_NAME: &str = "time";

#[derive(Debug)]
pub struct Schema {
    schema_ref: SchemaRef,
    // TODO(edd): column sort order??
}

impl Schema {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema_ref: schema }
    }

    pub fn schema_ref(&self) -> SchemaRef {
        self.schema_ref.clone()
    }

    pub fn cols(&self) -> usize {
        self.schema_ref.fields().len()
    }
}

/// A Segment is an immutable horizontal section (segment) of a table. By
/// definition it has the same schema as all the other segments in the table.
/// Further, all the columns within the segment have the same number of logical
/// rows.
///
/// This implementation will pull over the bulk of the prototype segment store
/// crate.
pub struct Segment {
    meta: MetaData,

    columns: Vec<Column>,
    all_columns_by_name: BTreeMap<String, usize>,
    tag_columns_by_name: BTreeMap<String, usize>,
    field_columns_by_name: BTreeMap<String, usize>,
    time_column: usize,
}

impl Segment {
    pub fn new(rows: u32, columns: BTreeMap<String, ColumnType>) -> Self {
        let mut meta = MetaData {
            rows,
            ..MetaData::default()
        };

        let mut segment_columns = vec![];
        let mut all_columns_by_name = BTreeMap::new();
        let mut tag_columns_by_name = BTreeMap::new();
        let mut field_columns_by_name = BTreeMap::new();
        let mut time_column = None;

        for (name, ct) in columns {
            meta.size += ct.size();
            match ct {
                ColumnType::Tag(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());
                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    tag_columns_by_name.insert(name, segment_columns.len());
                    segment_columns.push(c);
                }
                ColumnType::Field(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());
                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    field_columns_by_name.insert(name, segment_columns.len());
                    segment_columns.push(c);
                }
                ColumnType::Time(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.time_range = match c.column_range() {
                        None => panic!("time column must have non-null value"),
                        Some((
                            OwnedValue::Scalar(Scalar::I64(min)),
                            OwnedValue::Scalar(Scalar::I64(max)),
                        )) => (min, max),
                        Some((_, _)) => unreachable!("unexpected types for time range"),
                    };

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());

                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    time_column = Some(segment_columns.len());
                    segment_columns.push(c);
                }
            }
        }

        Self {
            meta,
            columns: segment_columns,
            all_columns_by_name,
            tag_columns_by_name,
            field_columns_by_name,
            time_column: time_column.unwrap(),
        }
    }

    /// The total size in bytes of the segment
    pub fn size(&self) -> u64 {
        self.meta.size
    }

    /// The number of rows in the segment (all columns have the same number of
    /// rows).
    pub fn rows(&self) -> u32 {
        self.meta.rows
    }

    /// The ranges on each column in the segment
    pub fn column_ranges(&self) -> &BTreeMap<String, (OwnedValue, OwnedValue)> {
        &self.meta.column_ranges
    }

    // Returns a reference to a column from the column name.
    fn column_by_name(&self, name: ColumnName<'_>) -> &Column {
        &self.columns[*self.all_columns_by_name.get(name).unwrap()]
    }

    // Returns a reference to the timestamp column.
    fn time_column(&self) -> &Column {
        &self.columns[self.time_column]
    }

    /// The time range of the segment (of the time column).
    pub fn time_range(&self) -> (i64, i64) {
        self.meta.time_range
    }

    /// Efficiently determine if the provided predicate might be satisfied by
    /// the provided column.
    pub fn column_could_satisfy_predicate(
        &self,
        column_name: ColumnName<'_>,
        predicate: &(Operator, Value<'_>),
    ) -> bool {
        self.meta
            .segment_could_satisfy_predicate(column_name, predicate)
    }

    ///
    /// Methods for reading the segment.
    ///

    /// Returns a set of materialised column values that satisfy a set of
    /// predicates.
    ///
    /// Right now, predicates are conjunctive (AND).
    pub fn read_filter<'a>(
        &self,
        columns: &[ColumnName<'a>],
        predicates: &[Predicate<'_>],
    ) -> ReadFilterResult<'a> {
        let row_ids = self.row_ids_from_predicates(predicates);
        ReadFilterResult(self.materialise_rows(columns, row_ids))
    }

    fn materialise_rows<'a>(
        &self,
        columns: &[ColumnName<'a>],
        row_ids: RowIDsOption,
    ) -> Vec<(ColumnName<'a>, Values)> {
        println!("row ids to materialise are {:?}", &row_ids);
        let mut results = vec![];
        match row_ids {
            RowIDsOption::None(_) => results, // nothing to materialise
            RowIDsOption::Some(row_ids) => {
                // TODO(edd): causes an allocation. Implement a way to pass a pooled
                // buffer to the croaring Bitmap API.
                let row_ids = row_ids.to_vec();
                for &col_name in columns {
                    let now = std::time::Instant::now();
                    let col = self.column_by_name(col_name);
                    results.push((col_name, col.values(row_ids.as_slice())));
                    println!("{} column materialising took {:?}", col_name, now.elapsed());
                }
                results
            }

            RowIDsOption::All(_) => {
                // TODO(edd): Perf - add specialised method to get all
                // materialised values from a column without having to
                // materialise a vector of row ids.......
                let row_ids = (0..self.rows()).collect::<Vec<_>>();

                for &col_name in columns {
                    let col = self.column_by_name(col_name);
                    results.push((col_name, col.values(row_ids.as_slice())));
                }
                results
            }
        }
    }

    // Determines the set of row ids that satisfy the time range and all of the
    // optional predicates.
    //
    // TODO(edd): right now `time_range` is special cased so we can use the
    // optimised execution path in the column to filter on a range. However,
    // eventually we should be able to express the time range as just another
    // one or two predicates.
    fn row_ids_from_predicates(&self, predicates: &[Predicate<'_>]) -> RowIDsOption {
        // TODO(edd): perf - pool the dst buffer so we can re-use it across
        // subsequent calls to `row_ids_from_predicates`.
        // Right now this buffer will be re-used across all columns in the
        // segment with predicates.
        let mut dst = RowIDs::new_bitmap();

        // find the time range predicates and execute a specialised range based
        // row id lookup.
        let time_predicates = predicates
            .iter()
            .filter(|(col_name, _)| col_name == &TIME_COLUMN_NAME)
            .collect::<Vec<_>>();
        assert!(time_predicates.len() == 2);

        let now = std::time::Instant::now();
        let time_row_ids = self.time_column().row_ids_filter_range(
            &time_predicates[0].1, // min time
            &time_predicates[1].1, // max time
            dst,
        );
        println!("Time column row ids took {:?}", now.elapsed());

        // TODO(edd): potentially pass this in so we can re-use it once we
        // have materialised any results.
        let mut result_row_ids = RowIDs::new_bitmap();

        match time_row_ids {
            // No matching rows based on time range - return buffer
            RowIDsOption::None(_) => return time_row_ids,

            // all rows match - continue to apply predicates
            RowIDsOption::All(_dst) => {
                dst = _dst; // hand buffer back
            }

            // some rows match - continue to apply predicates
            RowIDsOption::Some(row_ids) => {
                // union empty result set with matching timestamp rows
                result_row_ids.union(&row_ids);
                dst = row_ids // hand buffer back
            }
        }

        for (col_name, (op, value)) in predicates {
            if col_name == &TIME_COLUMN_NAME {
                continue; // we already processed the time column as a special case.
            }
            // N.B column should always exist because validation of
            // predicates should happen at the `Table` level.
            let col = self.column_by_name(col_name);

            // Explanation of how this buffer pattern works here. The idea is
            // that the buffer should be returned to the caller so it can be
            // re-used on other columns. To do that we need to hand the buffer
            // back even if we haven't populated it with any results.
            let now = std::time::Instant::now();
            match col.row_ids_filter(op, value, dst) {
                // No rows will be returned for the segment because this column
                // doe not match any rows.
                RowIDsOption::None(_dst) => return RowIDsOption::None(_dst),

                // Intersect the row ids found at this column with all those
                // found on other column predicates.
                RowIDsOption::Some(row_ids) => {
                    if result_row_ids.is_empty() {
                        result_row_ids.union(&row_ids)
                    }
                    result_row_ids.intersect(&row_ids);
                    dst = row_ids; // hand buffer back
                }

                // This is basically a no-op because all rows match the
                // predicate on this column.
                RowIDsOption::All(_dst) => {
                    dst = _dst; // hand buffer back
                }
            }
            println!("{} column row ids took {:?}", col_name, now.elapsed());
        }

        if result_row_ids.is_empty() {
            // All rows matched all predicates - return the empty buffer.
            return RowIDsOption::All(result_row_ids);
        }
        RowIDsOption::Some(result_row_ids)
    }

    /// Returns a set of group keys and aggregated column data associated with
    /// them.
    ///
    /// Right now, predicates are conjunctive (AND).
    #[allow(clippy::map_entry)]
    pub fn read_group<'a>(
        &'a self,
        predicates: &[Predicate<'a>],
        group_columns: &[ColumnName<'a>],
        aggregates: &[(ColumnName<'a>, AggregateType)],
    ) -> ReadGroupResult<'a> {
        let now = std::time::Instant::now();

        let row_ids = self.row_ids_from_predicates(predicates);
        let filter_row_ids = match row_ids {
            RowIDsOption::None(_) => return ReadGroupResult::default(), // no matching rows
            RowIDsOption::Some(row_ids) => Some(row_ids.to_vec()),
            RowIDsOption::All(row_ids) => None,
        };

        // materialise all encoded values for each column we are grouping on.
        let mut groupby_encoded_ids = Vec::with_capacity(group_columns.len());
        for name in group_columns {
            let col = self.column_by_name(name);
            let mut dst_buf = EncodedValues::with_capacity_u32(col.num_rows() as usize);

            // do we want some rows for the column or all of them?
            match &filter_row_ids {
                Some(row_ids) => {
                    dst_buf = col.encoded_values(row_ids, dst_buf);
                }
                None => {
                    // None implies "no partial set of row ids" meaning get all of them.
                    dst_buf = col.all_encoded_values(dst_buf);
                }
            }
            groupby_encoded_ids.push(dst_buf);
        }

        // materialise decoded values in aggregate columns.
        let mut aggregate_columns_data = Vec::with_capacity(aggregates.len());
        for (name, agg_type) in aggregates {
            let col = self.column_by_name(name);

            //
            // TODO(edd): this materialises a column per aggregate. If there are
            // multiple aggregates for the same column then this will over-allocate
            //
            // Do we want some rows for the column or all of them?
            let column_values = match &filter_row_ids {
                Some(row_ids) => col.values(row_ids),
                None => {
                    // None implies "no partial set of row ids" meaning get all of them.
                    col.all_values()
                }
            };
            aggregate_columns_data.push(column_values);
        }

        let mut groups = FxHashMap::default();
        let mut key_buf = Vec::with_capacity(group_columns.len());
        key_buf.resize(key_buf.capacity(), 0);

        let total_rows = groupby_encoded_ids[0].len();
        for row in 0..total_rows {
            for (j, col_ids) in groupby_encoded_ids.iter().enumerate() {
                match col_ids {
                    EncodedValues::I64(ids) => {
                        key_buf[j] = ids[row];
                    }
                    EncodedValues::U32(ids) => {
                        // TODO(edd): hmmmm. This is unfortunate - we only need
                        // the encoded values to be 64-bit integers if we are grouping
                        // by time (i64 column).
                        key_buf[j] = ids[row] as i64;
                    }
                }
            }

            if !groups.contains_key(&key_buf) {
                // Initialise any aggregates for the group key
                let mut group_key_aggs = Vec::with_capacity(aggregates.len());
                for (_, agg_type) in aggregates {
                    group_key_aggs.push(AggregateResult::from(agg_type));
                }

                for (i, values) in aggregate_columns_data.iter().enumerate() {
                    group_key_aggs[i].update(values.value(row));
                }

                groups.insert(key_buf.clone(), group_key_aggs);
                continue;
            }

            // Update all aggregates for the group key
            let group_key_aggs = groups.get_mut(&key_buf).unwrap();
            for (i, values) in aggregate_columns_data.iter().enumerate() {
                group_key_aggs[i].update(values.value(row));
            }
        }

        // Finally, build results set. Each encoded group key needs to be
        // materialised into a logical group key
        let col_buf = group_columns
            .iter()
            .map(|name| self.column_by_name(name))
            .collect::<Vec<_>>();
        let mut group_key_vec = Vec::with_capacity(groups.len());
        let mut aggregate_vec = Vec::with_capacity(groups.len());

        for (group_key, aggs) in groups.into_iter() {
            let mut logical_key = Vec::with_capacity(group_key.len());
            for (col_idx, &encoded_id) in group_key.iter().enumerate() {
                // TODO(edd): address the cast to u32
                logical_key.push(col_buf[col_idx].decode_id(encoded_id as u32));
            }

            group_key_vec.push(logical_key);
            aggregate_vec.push(aggs);
        }

        // ReadGroupResult {
        //     groupby_columns: group_columns,
        //     aggregate_columns: aggregates,
        //     group_keys: group_key_vec,
        //     aggregates: aggregate_vec,
        // }
        println!(
            "time: {:?} group keys {} aggs {}",
            now.elapsed(),
            group_key_vec.len(),
            aggregate_vec[0].len()
        );
        ReadGroupResult::default()
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.meta)?;
        for (name, column) in &self.all_columns_by_name {
            writeln!(f, "Column {}", name)?;
            writeln!(f, "{}\n", column)?;
        }
        Ok(())
    }
}

pub type Predicate<'a> = (ColumnName<'a>, (Operator, Value<'a>));

// A GroupKey is an ordered collection of row values. The order determines which
// columns the values originated from.
pub type GroupKey<'a> = Vec<Value<'a>>;

// A representation of a column name.
pub type ColumnName<'a> = &'a str;

/// The logical type that a column could have.
pub enum ColumnType {
    Tag(Column),
    Field(Column),
    Time(Column),
}

impl ColumnType {
    // The total size in bytes of the column
    pub fn size(&self) -> u64 {
        match &self {
            ColumnType::Tag(c) => c.size(),
            ColumnType::Field(c) => c.size(),
            ColumnType::Time(c) => c.size(),
        }
    }
}

// The GroupingStrategy determines which algorithm is used for calculating
// groups.
// enum GroupingStrategy {
//     // AutoGroup lets the executor determine the most appropriate grouping
//     // strategy using heuristics.
//     AutoGroup,

//     // HashGroup specifies that groupings should be done using a hashmap.
//     HashGroup,

//     // SortGroup specifies that groupings should be determined by first sorting
//     // the data to be grouped by the group-key.
//     SortGroup,
// }

#[derive(Default, Debug)]
struct MetaData {
    // The total size of the table in bytes.
    size: u64,

    // The total number of rows in the table.
    rows: u32,

    // The distinct set of columns for this table (all of these columns will
    // appear in all of the table's segments) and the range of values for
    // each of those columns.
    //
    // This can be used to skip the table entirely if a logical predicate can't
    // possibly match based on the range of values a column has.
    column_ranges: BTreeMap<String, (OwnedValue, OwnedValue)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: (i64, i64),
}

impl MetaData {
    // helper function to determine if the provided predicate could be satisfied by
    // the segment. If this function returns `false` then there is no point
    // attempting to read data from the segment.
    //
    pub fn segment_could_satisfy_predicate(
        &self,
        column_name: ColumnName<'_>,
        predicate: &(Operator, Value<'_>),
    ) -> bool {
        let (column_min, column_max) = match self.column_ranges.get(column_name) {
            Some(range) => range,
            None => return false, // column doesn't exist.
        };

        let (op, value) = predicate;
        match op {
            // If the column range covers the value then it could contain that value.
            Operator::Equal => column_min <= value && column_max >= value,

            // If every value in the column is equal to "value" then this will
            // be false, otherwise it must be satisfied
            Operator::NotEqual => (column_min != column_max) || column_max != value,

            // if the column max is larger than value then the column could
            // contain the value.
            Operator::GT => column_max > value,

            // if the column max is at least as large as `value` then the column
            // could contain the value.
            Operator::GTE => column_max >= value,

            // if the column min is smaller than value then the column could
            // contain the value.
            Operator::LT => column_min < value,

            // if the column min is at least as small as value then the column
            // could contain the value.
            Operator::LTE => column_min <= value,
        }
    }
}

impl std::fmt::Display for MetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Segment MetaData: size: {:?} rows: {:?} time range {:?}, ranges: {:?}",
            self.size, self.rows, self.time_range, self.column_ranges,
        )
    }
}

pub struct ReadFilterResult<'a>(pub Vec<(ColumnName<'a>, Values)>);

impl<'a> ReadFilterResult<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> std::fmt::Debug for ReadFilterResult<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // header line.
        for (i, (k, _)) in self.0.iter().enumerate() {
            write!(f, "{}", k)?;

            if i < self.0.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)?;

        // TODO: handle empty results?
        let expected_rows = self.0[0].1.len();
        let mut rows = 0;

        let mut iter_map = self
            .0
            .iter()
            .map(|(k, v)| (*k, ValuesIterator::new(v)))
            .collect::<BTreeMap<&str, ValuesIterator<'_>>>();

        while rows < expected_rows {
            if rows > 0 {
                writeln!(f)?;
            }

            for (i, (k, _)) in self.0.iter().enumerate() {
                if let Some(itr) = iter_map.get_mut(k) {
                    write!(f, "{}", itr.next().unwrap())?;
                    if i < self.0.len() - 1 {
                        write!(f, ",")?;
                    }
                }
            }

            rows += 1;
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct ReadGroupResult<'a> {
    // row-wise collection of group keys. Each group key contains column-wise
    // values for each of the groupby_columns.
    group_keys: Vec<GroupKey<'a>>,

    // row-wise collection of aggregates. Each aggregate contains column-wise
    // values for each of the aggregate_columns.
    aggregates: Vec<Vec<AggregateResult<'a>>>,
}

use std::iter::Iterator;
impl<'a> std::fmt::Display for ReadGroupResult<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: handle empty results?
        let expected_rows = self.group_keys.len();

        let mut row = 0;
        while row < expected_rows {
            if row > 0 {
                writeln!(f)?;
            }

            // write row for group by columns
            for value in &self.group_keys[row] {
                write!(f, "{},", value)?;
            }

            // write row for aggregate columns
            for (col_i, agg) in self.aggregates[row].iter().enumerate() {
                write!(f, "{}", agg)?;
                if col_i < self.aggregates[row].len() - 1 {
                    write!(f, ",")?;
                }
            }

            row += 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn build_predicates(
        from: i64,
        to: i64,
        column_predicates: Vec<Predicate<'_>>,
    ) -> Vec<Predicate<'_>> {
        let mut arr = vec![
            (
                TIME_COLUMN_NAME,
                (Operator::GTE, Value::Scalar(Scalar::I64(from))),
            ),
            (
                TIME_COLUMN_NAME,
                (Operator::LT, Value::Scalar(Scalar::I64(to))),
            ),
        ];

        arr.extend(column_predicates);
        arr
    }

    #[test]
    fn read_filter() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.insert("method".to_string(), mc);

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.insert("count".to_string(), fc);

        let segment = Segment::new(6, columns);

        let cases = vec![
            (
                vec!["count", "region", "time"],
                build_predicates(1, 6, vec![]),
                "count,region,time
100,west,1
101,west,2
200,east,3
203,west,4
203,south,5",
            ),
            (
                vec!["time", "region", "method"],
                build_predicates(-19, 2, vec![]),
                "time,region,method
1,west,GET",
            ),
            (
                vec!["time"],
                build_predicates(0, 3, vec![]),
                "time
1
2",
            ),
            (
                vec!["method"],
                build_predicates(0, 3, vec![]),
                "method
GET
POST",
            ),
            (
                vec!["count", "method", "time"],
                build_predicates(
                    0,
                    6,
                    vec![("method", (Operator::Equal, Value::String("POST")))],
                ),
                "count,method,time
101,POST,2
200,POST,3
203,POST,4",
            ),
            (
                vec!["region", "time"],
                build_predicates(
                    0,
                    6,
                    vec![("method", (Operator::Equal, Value::String("POST")))],
                ),
                "region,time
west,2
east,3
west,4",
            ),
        ];

        for (cols, predicates, expected) in cases {
            let results = segment.read_filter(&cols, &predicates);
            assert_eq!(format!("{:?}", results), expected);
        }

        // test no matching rows
        let results = segment.read_filter(
            &["method", "region", "time"],
            &build_predicates(-19, 1, vec![]),
        );
        let expected = "";
        assert!(results.is_empty());
    }

    #[test]
    fn read_group() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.insert("method".to_string(), mc);

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.insert("count".to_string(), fc);

        let segment = Segment::new(6, columns);

        let cases = vec![(
            build_predicates(1, 6, vec![]),
            vec!["region", "method"],
            vec![("count", AggregateType::Sum)],
            "region,method,count_sum
west,GET,100
west,POST,304
east,POST,200
south,PUT,203",
        )];

        for (predicate, group_cols, aggs, expected) in cases {
            let results = segment.read_group(&predicate, &group_cols, &aggs);
            assert_eq!(format!("{}", results), expected);
        }
    }

    #[test]
    fn segment_could_satisfy_predicate() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "GET", "GET", "GET", "GET", "GET"][..],
        ));
        columns.insert("method".to_string(), mc);

        let segment = Segment::new(6, columns);

        let cases = vec![
            ("az", &(Operator::Equal, Value::String("west")), false), // no az column
            ("region", &(Operator::Equal, Value::String("west")), true), // region column does contain "west"
            ("region", &(Operator::Equal, Value::String("over")), true), // region column might contain "over"
            ("region", &(Operator::Equal, Value::String("abc")), false), // region column can't contain "abc"
            ("region", &(Operator::Equal, Value::String("zoo")), false), // region column can't contain "zoo"
            (
                "region",
                &(Operator::NotEqual, Value::String("hello")),
                true,
            ), // region column might not contain "hello"
            ("method", &(Operator::NotEqual, Value::String("GET")), false), // method must only contain "GET"
            ("region", &(Operator::GT, Value::String("abc")), true), // region column might contain something > "abc"
            ("region", &(Operator::GT, Value::String("north")), true), // region column might contain something > "north"
            ("region", &(Operator::GT, Value::String("west")), false), // region column can't contain something > "west"
            ("region", &(Operator::GTE, Value::String("abc")), true), // region column might contain something ≥ "abc"
            ("region", &(Operator::GTE, Value::String("east")), true), // region column might contain something ≥ "east"
            ("region", &(Operator::GTE, Value::String("west")), true), // region column might contain something ≥ "west"
            ("region", &(Operator::GTE, Value::String("zoo")), false), // region column can't contain something ≥ "zoo"
            ("region", &(Operator::LT, Value::String("foo")), true), // region column might contain something < "foo"
            ("region", &(Operator::LT, Value::String("north")), true), // region column might contain something < "north"
            ("region", &(Operator::LT, Value::String("south")), true), // region column might contain something < "south"
            ("region", &(Operator::LT, Value::String("east")), false), // region column can't contain something < "east"
            ("region", &(Operator::LT, Value::String("abc")), false), // region column can't contain something < "abc"
            ("region", &(Operator::LTE, Value::String("east")), true), // region column might contain something ≤ "east"
            ("region", &(Operator::LTE, Value::String("north")), true), // region column might contain something ≤ "north"
            ("region", &(Operator::LTE, Value::String("south")), true), // region column might contain something ≤ "south"
            ("region", &(Operator::LTE, Value::String("abc")), false), // region column can't contain something ≤ "abc"
        ];

        for (column_name, predicate, exp) in cases {
            assert_eq!(
                segment.column_could_satisfy_predicate(column_name, predicate),
                exp,
                "({:?}, {:?}) failed",
                column_name,
                predicate
            );
        }
    }

    #[test]
    fn read_group_result_display() {
        let result = ReadGroupResult {
            group_keys: vec![
                vec![Value::String("east"), Value::String("host-a")],
                vec![Value::String("east"), Value::String("host-b")],
                vec![Value::String("west"), Value::String("host-a")],
                vec![Value::String("west"), Value::String("host-c")],
                vec![Value::String("west"), Value::String("host-d")],
            ],
            aggregates: vec![
                vec![
                    AggregateResult::Sum(Scalar::I64(10)),
                    AggregateResult::Count(3),
                ],
                vec![
                    AggregateResult::Sum(Scalar::I64(20)),
                    AggregateResult::Count(4),
                ],
                vec![
                    AggregateResult::Sum(Scalar::I64(25)),
                    AggregateResult::Count(3),
                ],
                vec![
                    AggregateResult::Sum(Scalar::I64(21)),
                    AggregateResult::Count(1),
                ],
                vec![
                    AggregateResult::Sum(Scalar::I64(11)),
                    AggregateResult::Count(9),
                ],
            ],
        };

        let expected = "east,host-a,10,3
east,host-b,20,4
west,host-a,25,3
west,host-c,21,1
west,host-d,11,9";

        assert_eq!(format!("{}", result), expected);
    }
}
