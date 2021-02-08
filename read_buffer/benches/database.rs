use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use arrow_deps::arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use data_types::schema::builder::SchemaBuilder;
use read_buffer::{BinaryExpr, Database, Predicate};

const BASE_TIME: i64 = 1351700038292387000_i64;
const ONE_MS: i64 = 1_000_000;

fn table_names(c: &mut Criterion) {
    let rb = generate_row_group(500_000);
    let db = Database::new();
    db.upsert_partition("hour_1", 0, "table_a", rb.clone());

    // no predicate - return all the tables
    benchmark_table_names(
        c,
        "database_table_names_all_tables",
        &db,
        &[0],
        Predicate::default(),
        1,
    );

    // predicate - meta data proves not present
    benchmark_table_names(
        c,
        "database_table_names_meta_pred_no_match",
        &db,
        &[0],
        Predicate::new(vec![BinaryExpr::from(("env", "=", "zoo"))]),
        0,
    );

    // predicate - single predicate match
    benchmark_table_names(
        c,
        "database_table_names_single_pred_match",
        &db,
        &[0],
        Predicate::new(vec![BinaryExpr::from(("env", "=", "prod"))]),
        1,
    );

    // predicate - multi predicate match
    benchmark_table_names(
        c,
        "database_table_names_multi_pred_match",
        &db,
        &[0],
        Predicate::new(vec![
            BinaryExpr::from(("env", "=", "prod")),
            BinaryExpr::from(("time", ">=", BASE_TIME)),
            BinaryExpr::from(("time", "<", BASE_TIME + (ONE_MS * 10000))),
        ]),
        1,
    );

    // Add the table into ten more chunks. Show that performance is not impacted.
    for chunk_id in 1..=10 {
        db.upsert_partition("hour_1", chunk_id, "table_a", rb.clone());
    }

    // predicate - multi predicate match on all chunks
    benchmark_table_names(
        c,
        "database_table_names_multi_pred_match_multi_tables",
        &db,
        &(0..=10).into_iter().collect::<Vec<_>>(),
        Predicate::new(vec![
            BinaryExpr::from(("env", "=", "prod")),
            BinaryExpr::from(("time", ">=", BASE_TIME)),
            BinaryExpr::from(("time", "<", BASE_TIME + (ONE_MS * 10000))),
        ]),
        1,
    );
}

fn benchmark_table_names(
    c: &mut Criterion,
    bench_name: &str,
    database: &Database,
    chunk_ids: &[u32],
    predicate: Predicate,
    expected_rows: usize,
) {
    c.bench_function(bench_name, |b| {
        b.iter_batched(
            || predicate.clone(), // don't want to time predicate cloning
            |predicate: Predicate| {
                let tables = database
                    .table_names("hour_1", chunk_ids, predicate)
                    .unwrap();
                assert_eq!(tables.num_rows(), expected_rows);
            },
            BatchSize::SmallInput,
        );
    });
}

// generate a row group with three columns of varying cardinality.
fn generate_row_group(rows: usize) -> RecordBatch {
    let schema = SchemaBuilder::new()
        .non_null_tag("env")
        .non_null_tag("container_id")
        .timestamp()
        .build()
        .unwrap();

    let container_ids = (0..rows)
        .into_iter()
        .map(|i| format!("my_container_{:?}", i))
        .collect::<Vec<_>>();

    let data: Vec<ArrayRef> = vec![
        // sorted 2 cardinality column
        Arc::new(StringArray::from(
            (0..rows)
                .into_iter()
                .map(|i| if i < rows / 2 { "prod" } else { "dev" })
                .collect::<Vec<_>>(),
        )),
        // completely unique cardinality column
        Arc::new(StringArray::from(
            container_ids
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
        )),
        // ms increasing time column;
        Arc::new(Int64Array::from(
            (0..rows)
                .into_iter()
                .map(|i| BASE_TIME + (i as i64 * ONE_MS))
                .collect::<Vec<_>>(),
        )),
    ];

    RecordBatch::try_new(schema.into(), data).unwrap()
}

criterion_group!(benches, table_names);
criterion_main!(benches);
