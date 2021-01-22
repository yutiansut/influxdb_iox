#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use std::{env, f64, sync::Arc};
pub use tempfile;

pub mod tracing;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// A test helper function for asserting floating point numbers are within the
/// machine epsilon because strict comparison of floating point numbers is
/// incorrect
pub fn approximately_equal(f1: f64, f2: f64) -> bool {
    (f1 - f2).abs() < f64::EPSILON
}

pub fn all_approximately_equal(f1: &[f64], f2: &[f64]) -> bool {
    f1.len() == f2.len() && f1.iter().zip(f2).all(|(&a, &b)| approximately_equal(a, b))
}

/// Return a temporary directory that is deleted when the object is dropped
pub fn tmp_dir() -> Result<tempfile::TempDir> {
    let _ = dotenv::dotenv();

    let root = env::var_os("TEST_INFLUXDB_IOX_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("influxdb_iox")
        .tempdir_in(root)?)
}

pub fn tmp_file() -> Result<tempfile::NamedTempFile> {
    let _ = dotenv::dotenv();

    let root = env::var_os("TEST_INFLUXDB_IOX_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("influxdb_iox")
        .tempfile_in(root)?)
}

/// Writes the specified string to a new temporary file, returning the Path to
/// the file
pub fn make_temp_file<C: AsRef<[u8]>>(contents: C) -> tempfile::NamedTempFile {
    let file = tmp_file().expect("creating temp file");

    std::fs::write(&file, contents).expect("writing data to temp file");
    file
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_vec_to_arc_vec(str_vec: &[&str]) -> Arc<Vec<Arc<String>>> {
    Arc::new(str_vec.iter().map(|s| Arc::new(String::from(*s))).collect())
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_pair_vec_to_vec(str_vec: &[(&str, &str)]) -> Vec<(Arc<String>, Arc<String>)> {
    str_vec
        .iter()
        .map(|(s1, s2)| (Arc::new(String::from(*s1)), Arc::new(String::from(*s2))))
        .collect()
}

/// Converts bytes representing tag_keys values to Rust strings,
/// handling the special case _m (0x00) and _f (0xff) values. Other
/// than [0xff] panics on any non-utf8 string.
pub fn tag_key_bytes_to_strings(bytes: Vec<u8>) -> String {
    match bytes.as_slice() {
        [0] => "_m(0x00)".into(),
        // note this isn't valid UTF8 and thus would assert below
        [255] => "_f(0xff)".into(),
        _ => String::from_utf8(bytes).expect("string value response was not utf8"),
    }
}

pub fn enable_logging() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
}

#[macro_export]
/// A macro to assert that one string is contained within another with
/// a nice error message if they are not.
///
/// Usage: `assert_contains!(actual, expected)`
///
/// Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertable into Strings (Into<String>)
macro_rules! assert_contains {
    ($ACTUAL: expr, $EXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let expected_value: String = $EXPECTED.into();
        assert!(
            actual_value.contains(&expected_value),
            "Can not find expected in actual.\n\nExpected:\n{}\n\nActual:\n{}",
            expected_value,
            actual_value
        );
    };
}

#[macro_export]
/// A macro to assert that one string is NOT contained within another with
/// a nice error message if that check fails. Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertable into Strings (Into<String>)
macro_rules! assert_not_contains {
    ($ACTUAL: expr, $UNEXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let unexpected_value: String = $UNEXPECTED.into();
        assert!(
            !actual_value.contains(&unexpected_value),
            "Found unexpected value in actual.\n\nUnexpected:\n{}\n\nActual:\n{}",
            unexpected_value,
            actual_value
        );
    };
}
