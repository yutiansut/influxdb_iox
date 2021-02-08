//! A collection of testing functions for arrow based code
use arrow::record_batch::RecordBatch;

use crate::arrow::compute::kernels::sort::{lexsort, SortColumn, SortOptions};

/// Compares the formatted output with the pretty formatted results of
/// record batches. This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
/// assert_table_eq(expected_lines: &[&str], chunks: &[RecordBatch])
#[macro_export]
macro_rules! assert_table_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.into_iter().map(|s| s.to_string()).collect();

        let formatted = arrow_deps::arrow::util::pretty::pretty_format_batches($CHUNKS).unwrap();

        let actual_lines = formatted.trim().split('\n').collect::<Vec<_>>();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

// sort a record batch by all columns (to provide a stable output order for test
// comparison)
pub fn sort_record_batch(batch: RecordBatch) -> RecordBatch {
    let sort_input: Vec<SortColumn> = batch
        .columns()
        .iter()
        .map(|col| SortColumn {
            values: col.clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect();

    let sort_output = lexsort(&sort_input).expect("Sorting to complete");

    RecordBatch::try_new(batch.schema(), sort_output).unwrap()
}
