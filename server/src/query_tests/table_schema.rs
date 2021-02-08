//! Tests for the table_names implementation

use arrow_deps::arrow::datatypes::DataType;
use data_types::{schema::builder::SchemaBuilder, selection::Selection};
use query::{Database, PartitionChunk};

use super::scenarios::*;

/// Creates and loads several database scenarios using the db_setup
/// function.
///
/// runs table_schema(predicate) and compares it to the expected
/// output
macro_rules! run_table_schema_test_case {
    ($DB_SETUP:expr, $SELECTION:expr, $TABLE_NAME:expr, $EXPECTED_SCHEMA:expr) => {
        let selection = $SELECTION;
        let table_name = $TABLE_NAME;
        let expected_schema = $EXPECTED_SCHEMA;

        for scenario in $DB_SETUP.make().await {
            let DBScenario { scenario_name, db } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!(
                "Getting schema for table '{}', selection {:?}",
                table_name, selection
            );

            // Make sure at least one table has data
            let mut chunks_with_table = 0;

            for partition_key in db.partition_keys().await.unwrap() {
                for chunk in db.chunks(&partition_key).await {
                    if chunk.has_table(table_name).await {
                        chunks_with_table += 1;
                        let actual_schema = chunk
                            .table_schema(table_name, selection.clone())
                            .await
                            .unwrap();

                        assert_eq!(
                            expected_schema,
                            actual_schema,
                            "Mismatch in chunk {}\nExpected:\n{:#?}\nActual:\n{:#?}\n",
                            chunk.id(),
                            expected_schema,
                            actual_schema
                        );
                    }
                }
                assert!(
                    chunks_with_table > 0,
                    "Expected at least one chunk to have data, but none did"
                );
            }
        }
    };
}

#[tokio::test]
async fn list_schema_cpu_all() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .tag("region")
        .timestamp()
        .field("user", DataType::Float64)
        .build()
        .unwrap();

    run_table_schema_test_case!(TwoMeasurements {}, Selection::All, "cpu", expected_schema);
}

#[tokio::test]
async fn list_schema_disk_all() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .field("bytes", DataType::Int64)
        .tag("region")
        .timestamp()
        .build()
        .unwrap();

    run_table_schema_test_case!(TwoMeasurements {}, Selection::All, "disk", expected_schema);
}

#[tokio::test]
async fn list_schema_cpu_selection() {
    let expected_schema = SchemaBuilder::new()
        .field("user", DataType::Float64)
        .tag("region")
        .build()
        .unwrap();

    // Pick an order that is not lexographic
    let selection = Selection::Some(&["user", "region"]);

    run_table_schema_test_case!(TwoMeasurements {}, selection, "cpu", expected_schema);
}

#[tokio::test]
async fn list_schema_disk_selection() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .timestamp()
        .field("bytes", DataType::Int64)
        .build()
        .unwrap();

    // Pick an order that is not lexographic
    let selection = Selection::Some(&["time", "bytes"]);

    run_table_schema_test_case!(TwoMeasurements {}, selection, "disk", expected_schema);
}
