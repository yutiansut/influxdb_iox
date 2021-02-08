use std::{convert::TryFrom, fmt::Display};

use arrow_deps::arrow;
use data_types::schema::InfluxFieldType;

/// A schema that is used to track the names and semantics of columns returned
/// in results out of various operations on a row group.
///
/// This schema is useful for helping with displaying information in tests and
/// decorating Arrow record batches when results are converted before leaving
/// the read buffer.
#[derive(Default, PartialEq, Debug, Clone)]
pub struct ResultSchema {
    pub select_columns: Vec<(ColumnType, LogicalDataType)>,
    pub group_columns: Vec<(ColumnType, LogicalDataType)>,
    pub aggregate_columns: Vec<(ColumnType, AggregateType, LogicalDataType)>,
}

impl ResultSchema {
    pub fn select_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.select_columns.iter().map(|(name, _)| match name {
            ColumnType::Tag(name) => name,
            ColumnType::Field(name) => name,
            ColumnType::Timestamp(name) => name,
            ColumnType::Other(name) => name,
        })
    }

    pub fn group_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.group_columns.iter().map(|(name, _)| match name {
            ColumnType::Tag(name) => name,
            ColumnType::Field(name) => name,
            ColumnType::Timestamp(name) => name,
            ColumnType::Other(name) => name,
        })
    }

    pub fn aggregate_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.aggregate_columns
            .iter()
            .map(|(name, _, _)| match name {
                ColumnType::Tag(name) => name,
                ColumnType::Field(name) => name,
                ColumnType::Timestamp(name) => name,
                ColumnType::Other(name) => name,
            })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.select_columns.len() + self.group_columns.len() + self.aggregate_columns.len()
    }

    // How to display the name for a column that was constructed as an aggregate
    // result.
    //
    // TODO(edd): support multiple instances of the same aggregation on the same
    // column? E.g., `temp_sum_1`, `temp_sum_2` etc??
    fn aggregate_result_column_name(&self, i: usize) -> String {
        let (col_type, agg_type, _) = self.aggregate_columns.get(i).unwrap();
        format!("{}_{}", col_type, agg_type)
    }
}

/// Effectively emits a header line for a CSV-like table.
impl Display for ResultSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // do we need to emit the group by or aggregate columns?
        let has_agg_columns = !self.aggregate_columns.is_empty();

        for (i, (name, _)) in self.select_columns.iter().enumerate() {
            if has_agg_columns || i < self.select_columns.len() - 1 {
                write!(f, "{},", name)?;
            } else if !has_agg_columns {
                return write!(f, "{}", name); // last value in header row
            }
        }

        // write out group by columns, if any
        for (i, (name, _)) in self.group_columns.iter().enumerate() {
            write!(f, "{},", name)?;
        }

        // finally, emit the aggregate columns
        for (i, _) in self.aggregate_columns.iter().enumerate() {
            write!(f, "{}", self.aggregate_result_column_name(i))?;

            if i < self.aggregate_columns.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)
    }
}

impl TryFrom<&ResultSchema> for data_types::schema::Schema {
    type Error = data_types::schema::builder::Error;

    fn try_from(rs: &ResultSchema) -> Result<Self, Self::Error> {
        let mut builder = data_types::schema::builder::SchemaBuilder::new();
        for (col_type, data_type) in &rs.select_columns {
            match col_type {
                ColumnType::Tag(name) => builder = builder.tag(name.as_str()),
                ColumnType::Field(name) => {
                    builder = builder.influx_field(name.as_str(), data_type.into())
                }
                ColumnType::Timestamp(_) => builder = builder.timestamp(),
                ColumnType::Other(name) => builder = builder.field(name.as_str(), data_type.into()),
            }
        }

        for (col_type, data_type) in &rs.group_columns {
            match col_type {
                ColumnType::Tag(name) => builder = builder.tag(name.as_str()),
                ColumnType::Field(name) => {
                    builder = builder.influx_field(name.as_str(), data_type.into())
                }
                ColumnType::Timestamp(_) => builder = builder.timestamp(),
                ColumnType::Other(name) => builder = builder.field(name.as_str(), data_type.into()),
            }
        }

        for (i, (col_type, _, data_type)) in rs.aggregate_columns.iter().enumerate() {
            let col_name = rs.aggregate_result_column_name(i);

            match col_type {
                ColumnType::Field(_) => {
                    builder = builder.influx_field(col_name.as_str(), data_type.into())
                }
                ColumnType::Other(_) => {
                    builder = builder.field(col_name.as_str(), data_type.into())
                }
                ct => unreachable!("not possible to aggregate {:?} columns", ct),
            }
        }

        builder.build()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The logical data-type for a column.
pub enum LogicalDataType {
    Integer,  // Signed integer
    Unsigned, // Unsigned integer
    Float,    //
    String,   // UTF-8 valid string
    Binary,   // Arbitrary collection of bytes
    Boolean,  //
}

impl From<&LogicalDataType> for arrow::datatypes::DataType {
    fn from(logical_type: &LogicalDataType) -> Self {
        match logical_type {
            LogicalDataType::Integer => arrow::datatypes::DataType::Int64,
            LogicalDataType::Unsigned => arrow::datatypes::DataType::UInt64,
            LogicalDataType::Float => arrow::datatypes::DataType::Float64,
            LogicalDataType::String => arrow::datatypes::DataType::Utf8,
            LogicalDataType::Binary => arrow::datatypes::DataType::Binary,
            LogicalDataType::Boolean => arrow::datatypes::DataType::Boolean,
        }
    }
}

impl From<&LogicalDataType> for InfluxFieldType {
    fn from(logical_type: &LogicalDataType) -> Self {
        match logical_type {
            LogicalDataType::Integer => InfluxFieldType::Integer,
            LogicalDataType::Unsigned => InfluxFieldType::UInteger,
            LogicalDataType::Float => InfluxFieldType::Float,
            LogicalDataType::String => InfluxFieldType::String,
            LogicalDataType::Binary => {
                unimplemented!("binary data type cannot be represented as InfluxFieldType")
            }
            LogicalDataType::Boolean => InfluxFieldType::Boolean,
        }
    }
}

/// These variants describe supported aggregates that can applied to columnar
/// data in the Read Buffer.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum AggregateType {
    Count,
    First,
    Last,
    Min,
    Max,
    Sum,
    /* TODO - support:
     * Distinct - (edd): not sure this counts as an aggregations. Seems more like a special
     * filter. CountDistinct
     * Percentile */
}

impl std::fmt::Display for AggregateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AggregateType::Count => "count",
                AggregateType::First => "first",
                AggregateType::Last => "last",
                AggregateType::Min => "min",
                AggregateType::Max => "max",
                AggregateType::Sum => "sum",
            }
        )
    }
}

/// Describes the semantic meaning of the column in a set of results. That is,
/// whether the column is a "tag", "field", "timestamp", or "other".
#[derive(PartialEq, Debug, PartialOrd, Clone)]
pub enum ColumnType {
    Tag(String),
    Field(String),
    Timestamp(String),
    Other(String),
}

impl ColumnType {
    pub fn as_str(&self) -> &str {
        match self {
            ColumnType::Tag(name) => name.as_str(),
            ColumnType::Field(name) => name.as_str(),
            ColumnType::Timestamp(name) => name.as_str(),
            ColumnType::Other(name) => name.as_str(),
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ColumnType::Tag(name) => name,
                ColumnType::Field(name) => name,
                ColumnType::Timestamp(name) => name,
                ColumnType::Other(name) => name,
            }
        )
    }
}
