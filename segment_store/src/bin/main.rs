#![allow(dead_code)]
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use rand_distr::{Distribution, Normal};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use arrow_deps::{
    arrow,
    arrow::array,
    arrow::ipc::{reader, writer},
    arrow::record_batch::RecordBatch,
};
use packers::{sorter, Packer, Packers};

use segment_store::column::cmp::Operator;
use segment_store::column::{AggregateType, Scalar, Value};
use segment_store::{table, table::ColumnType};

const ONE_MS: i64 = 1_000_000;
const ONE_HOUR: i64 = ONE_MS * 3_600_000;
const START_TIME: i64 = 1604188800000000000_i64;

// determines how many rows will be in a single segment, which is set to one
// hour.
const ROWS_PER_HOUR: usize = 10_000_000;

// minimum and maximum number of spans in a trace
const SPANS_MIN: usize = 10;
const SPANS_MAX: usize = 11;

const HOURS: usize = 1;

fn main() {
    let mut rng = rand::thread_rng();

    let column_names = vec![
        ColumnType::Tag("env".to_string()),
        ColumnType::Tag("data_centre".to_string()),
        ColumnType::Tag("cluster".to_string()),
        ColumnType::Tag("user_id".to_string()),
        ColumnType::Tag("request_id".to_string()),
        ColumnType::Tag("trace_id".to_string()),
        ColumnType::Tag("node_id".to_string()),
        ColumnType::Tag("pod_id".to_string()),
        ColumnType::Tag("span_id".to_string()),
        ColumnType::Field("duration".to_string()),
        ColumnType::Time,
    ];

    let packers = generate_packers(START_TIME, ROWS_PER_HOUR, &mut rng);

    // sort packer tags so they're in lexicographic order.
    let packers = vec![
        packers.get(2).unwrap(),
        packers.get(1).unwrap(),
        packers.get(0).unwrap(),
        packers.get(6).unwrap(),
        packers.get(7).unwrap(),
        packers.get(4).unwrap(),
        packers.get(8).unwrap(),
        packers.get(5).unwrap(),
        packers.get(3).unwrap(),
        packers.get(9).unwrap(),
        packers.get(10).unwrap(),
    ];

    print_segment_line_protocol(
        &[
            "cluster",
            "data_centre",
            "env",
            "node_id",
            "pod_id",
            "request_id",
            "span_id",
            "trace_id",
            "user_id",
            "duration",
            "time",
        ],
        packers,
        "traces",
        0..9,
        9..10,
        10,
    );

    // uncomment this to generate a record batch.
    // let (rb, sample_trace_id) = generate_record_batch(&mut rng);
    // println!("Saving Arrow file");
    // save_record_batch(&rb[0]);

    // uncomment this to load record batch from file.
    // let sample_trace_id = "zzzzm6FK".to_string();
    // let rb = load_record_batch("/Users/edd/tracing_10m-g6oHreN9.arrow");

    // let now = Instant::now();
    // let table = table::Table::with_record_batch("tracing".to_string(), column_names, &rb[0]);
    // println!("Record batch to segment took {:?}", now.elapsed());
    // println!("Segment size is {:?} bytes", table.size());

    // // loop {
    // for _ in 0..1000000 {
    //     // execute_select(&table, sample_trace_id.as_str());
    //     // println!("{}", execute_read_group(&table));
    //     println!(
    //         "{}",
    //         execute_read_group_rle_only(
    //             &table,
    //             &["data_centre", "cluster"],
    //             &[("duration", AggregateType::Count)]
    //         )
    //     );

    //     // println!("{:?}", results);
    // }
}

fn execute_select<'a>(table: &'a table::Table, sample_trace_id: &'a str) {
    let now = Instant::now();

    let predicates = vec![
        (
            "trace_id",
            (Operator::Equal, Value::String(sample_trace_id)),
        ),
        ("time", (Operator::GTE, Value::Scalar(Scalar::I64(0)))),
        ("time", (Operator::LT, Value::Scalar(Scalar::I64(i64::MAX)))),
    ];

    let res = table.select(
        &[
            "env",
            "data_centre",
            "cluster",
            "user_id",
            "request_id",
            "trace_id",
            "node_id",
            "pod_id",
            "span_id",
            "duration",
            "time",
        ],
        predicates.as_slice(),
    );

    println!(
        "executed select in {:?} for {:?} results",
        now.elapsed(),
        res.is_empty()
    );
}

fn execute_read_group<'a>(
    table: &'a table::Table,
    group_cols: &'a [&str],
    aggregates: &'a [(&str, AggregateType)],
) -> table::ReadGroupResults<'a> {
    let now = Instant::now();

    let res = table.aggregate(
        &[
            ("time", (Operator::GTE, Value::Scalar(Scalar::I64(0)))),
            ("time", (Operator::LT, Value::Scalar(Scalar::I64(i64::MAX)))),
        ],
        group_cols,
        aggregates,
    );

    println!("executed read group in {:?}", now.elapsed());
    res
}

fn execute_read_group_rle_only<'a>(
    table: &'a table::Table,
    group_cols: &'a [&str],
    aggregates: &'a [(&str, AggregateType)],
) -> table::ReadGroupResults<'a> {
    let now = Instant::now();

    let res = table.aggregate(&[], group_cols, aggregates);

    println!(
        "executed read group rle-only in {:?} for {:?} groups",
        now.elapsed(),
        res.values[0].cardinality()
    );
    table::ReadGroupResults::default()
}

// generates a record batch of test data and rather spuriously returns a sample
// trace id.
fn generate_record_batch(rng: &mut ThreadRng) -> (Vec<RecordBatch>, String) {
    let now = std::time::Instant::now();
    let mut packers = generate_packers(START_TIME, ROWS_PER_HOUR, rng);
    let middle_trace_id = packers[5]
        .str_packer()
        .get(ROWS_PER_HOUR / 2)
        .unwrap()
        .to_owned();

    println!(
        "generating segment took {:?} - sample trace_id is {}",
        now.elapsed(),
        middle_trace_id
    );

    // COLUMN ORDERS:
    // env            - 0
    // data_centre,   - 1
    // cluster,       - 2
    // user_id,       - 3
    // request_id,    - 4
    // trace_id,      - 5
    // node_id,       - 6
    // pod_id,        - 7
    // span_id        - 8
    // duration,      - 9
    // time,          - 10
    //
    // No point ordering on trace_id or span_id
    let now = std::time::Instant::now();
    sorter::sort(&mut packers, &[0, 1, 2, 6, 7, 3, 4, 10]).unwrap();
    println!("sorting took {:?}", now.elapsed());

    let now = Instant::now();
    let rb = packers_to_record_batch(
        vec![
            "env",
            "data_centre",
            "cluster",
            "user_id",
            "request_id",
            "trace_id",
            "node_id",
            "pod_id",
            "span_id",
            "duration",
            "time",
        ],
        packers,
    );
    println!("Packers to arrow rb took {:?}", now.elapsed());

    (vec![rb], middle_trace_id)
}

fn generate_packers(start_time: i64, rows_per_hour: usize, rng: &mut ThreadRng) -> Vec<Packers> {
    let mut segment = Vec::new();

    // 9 tag columns: env, data_centre, cluster, user_id, request_id, trace_id, node_id, pod_id, span_id
    for _ in 0..9 {
        segment.push(Packers::String(Packer::<String>::new()));
    }

    // A duration "field" column
    segment.push(Packers::Integer(Packer::<i64>::new()));

    // A timestamp column.
    segment.push(Packers::Integer(Packer::<i64>::new()));

    let traces_to_generate = rows_per_hour / ((SPANS_MIN + SPANS_MAX) / 2);
    let trace_start_time_spacing = (ONE_HOUR - (1000 * ONE_MS)) / traces_to_generate as i64; // back off a second to not overlap boundary

    // println!(
    //     "generating {:?} traces with spacing {:?} ns",
    //     traces_to_generate, trace_start_time_spacing
    // );
    for i in 0..traces_to_generate as i64 {
        // append a trace to the columns in a segment.
        //
        // For each trace take the hour "start time" and spread each trace
        // evenly through that hour.
        segment = generate_trace(
            start_time + (i * trace_start_time_spacing),
            rng.gen_range(SPANS_MIN, SPANS_MAX + 1),
            rng,
            segment,
        );
    }

    segment
}

fn generate_trace(
    timestamp: i64,
    spans: usize,
    rng: &mut ThreadRng,
    mut table: Vec<Packers>,
) -> Vec<Packers> {
    let env_value = rng.gen_range(0_u8, 2);
    let env = format!("env-{:?}", env_value); // cardinality of 2.

    let data_centre_value = rng.gen_range(0_u8, 10);
    let data_centre = format!("data_centre-{:?}-{:?}", env_value, data_centre_value); // cardinality of 2 * 10  = 20

    let cluster_value = rng.gen_range(0_u8, 10);
    let cluster = format!(
        "cluster-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value // cardinality of 2 * 10 * 10 = 200
    );

    // user id is dependent on the cluster
    let user_id_value = rng.gen_range(0_u32, 1000);
    let user_id = format!(
        "uid-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value // cardinality of 2 * 10 * 10 * 1000 = 200,000
    );

    let request_id_value = rng.gen_range(0_u32, 10);
    let request_id = format!(
        "rid-{:?}-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value,
        request_id_value // cardinality of 2 * 10 * 10 * 1000 * 10 = 2,000,000
    );

    let trace_id = rng.sample_iter(&Alphanumeric).take(8).collect::<String>();

    let fixed_values = vec![env, data_centre, cluster, user_id, request_id, trace_id];
    let node_id_col = fixed_values.len();
    let pod_id_col = node_id_col + 1;
    let span_id_col = node_id_col + 2;
    let duration_field_col = node_id_col + 3;
    let timestamp_col = node_id_col + 4;

    // for the first 6 columns append the fixed generated values "spans many" times
    // so that there are "spans new rows" in each of the columns.
    let mut buffer: Vec<String> = Vec::with_capacity(spans);
    for (i, value) in fixed_values.into_iter().enumerate() {
        buffer.clear();
        buffer.resize(spans, value.to_string());

        table[i].str_packer_mut().extend_from_slice(&buffer);
    }

    // the trace should move across hosts, which in this example would be
    // nodes and pods.
    buffer.clear(); // use this buffer for node_id column.
    let mut pod_buffer = Vec::with_capacity(spans);
    let node_id_prefix = format!("{}-{}-{}", env_value, data_centre_value, cluster_value,);
    for _ in 0..spans {
        // each node the trace hits is in the same cluster...
        let node_id = rng.gen_range(0, 10); // cardinality is 2 * 10 * 10 * 10 = 2,000
        buffer.push(format!("node_id-{}-{}", node_id_prefix, node_id));

        pod_buffer.push(format!(
            "pod_id-{}-{}-{}",
            node_id_prefix,
            node_id,
            rng.gen_range(0, 10) // cardinality is 2 * 10 * 10 * 10 * 10 = 20,000
        ));
    }
    table[node_id_col]
        .str_packer_mut()
        .extend_from_slice(buffer.as_slice());
    table[pod_id_col]
        .str_packer_mut()
        .extend_from_slice(pod_buffer.as_slice());

    // randomly generate span ids.
    buffer.clear();
    for _ in 0..spans {
        let id = rng.sample_iter(&Alphanumeric).take(8).collect::<String>();
        buffer.push(id);
    }
    table[span_id_col]
        .str_packer_mut()
        .extend_from_slice(&buffer);

    // randomly generate some duration times in milliseconds.
    let normal = Normal::new(10.0, 5.0).unwrap();
    let durations = (0..spans)
        .map(|_| {
            (normal.sample(rng) * ONE_MS as f64)
                .max(ONE_MS as f64) // minimum duration is 1ms
                .round() as i64
        })
        .collect::<Vec<_>>();
    table[duration_field_col]
        .i64_packer_mut()
        .extend_from_slice(&durations);

    // write same timestamp.
    let mut times = std::iter::repeat(timestamp).take(spans).collect::<Vec<_>>();

    // adjust times by the durations.
    let mut total_duration = 0;
    for i in 0..times.len() {
        times[i] += total_duration;
        total_duration += durations[i];
    }
    table[timestamp_col]
        .i64_packer_mut()
        .extend_from_slice(&times);

    // Return the updated table back
    table
}

// determine the arrow schema type from a packer column
fn arrow_datatype(col: &Packers) -> arrow::datatypes::DataType {
    match col {
        Packers::Float(_) => arrow::datatypes::DataType::Float64,
        Packers::Integer(_) => arrow::datatypes::DataType::Int64,
        Packers::Bytes(_) | Packers::String(_) => arrow::datatypes::DataType::Utf8,
        Packers::Boolean(_) => arrow::datatypes::DataType::Boolean,
    }
}

fn packers_to_record_batch(col_names: Vec<&str>, columns: Vec<Packers>) -> RecordBatch {
    let mut record_batch_fields: Vec<arrow::datatypes::Field> = vec![];

    for (i, column) in columns.iter().enumerate() {
        let nullable = false; // columns not nullable in test

        let field = arrow::datatypes::Field::new(col_names[i], arrow_datatype(column), nullable);

        record_batch_fields.push(field);
    }
    println!("{:?}", record_batch_fields);

    let schema = arrow::datatypes::Schema::new(record_batch_fields);

    let mut record_batch_arrays: Vec<arrow::array::ArrayRef> = vec![];

    for column in columns {
        match column {
            Packers::Float(p) => {
                record_batch_arrays.push(Arc::new(array::Float64Array::from(p.owned())));
            }
            Packers::Integer(p) => {
                record_batch_arrays.push(Arc::new(array::Int64Array::from(p.owned())));
            }
            Packers::Bytes(p) => {
                let mut builder = array::StringBuilder::new(p.num_rows());
                for v in p.values() {
                    match v {
                        Some(v) => {
                            builder.append_value(v.as_utf8().unwrap()).unwrap();
                        }
                        None => {
                            builder.append_null().unwrap();
                        }
                    }
                }
                let array = builder.finish();
                record_batch_arrays.push(Arc::new(array));
            }
            Packers::String(p) => {
                let mut builder = array::StringBuilder::new(p.num_rows());
                for v in p.values() {
                    match v {
                        Some(v) => {
                            builder.append_value(v.as_str()).unwrap();
                        }
                        None => {
                            builder.append_null().unwrap();
                        }
                    }
                }
                let array = builder.finish();
                record_batch_arrays.push(Arc::new(array));
            }
            Packers::Boolean(p) => {
                let array = array::BooleanArray::from(p.owned());
                record_batch_arrays.push(Arc::new(array));
            }
        }
    }

    RecordBatch::try_new(Arc::new(schema), record_batch_arrays).unwrap()
}

fn print_segment(segment: &mut Vec<Packers>) {
    let total_rows = segment[0].num_rows();
    let mut rows = 0;

    let mut col_itrs = segment.iter_mut().map(|p| p.iter()).collect::<Vec<_>>();

    while rows < total_rows {
        if rows > 0 {
            println!();
        }

        for itr in col_itrs.iter_mut() {
            match itr {
                packers::packers::PackersIterator::Float(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Integer(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Bytes(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v.as_utf8().unwrap()),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::String(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v.as_str()),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Boolean(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
            }
        }
        rows += 1;
    }
}

fn print_segment_line_protocol(
    col_names: &[&str],
    col_data: Vec<&Packers>,
    measurement: &str,
    tag_range: std::ops::Range<usize>,
    field_range: std::ops::Range<usize>,
    time_col: usize,
) {
    let total_rows = col_data[0].num_rows();
    let mut rows = 0;

    let mut tag_col_itrs = vec![];
    let mut field_col_itrs = vec![];
    let mut time_col_itr = None;

    for (i, p) in col_data.iter().enumerate() {
        if tag_range.contains(&i) {
            println!("pushing tag {:?}", col_names[i]);
            tag_col_itrs.push(p.iter());
        } else if field_range.contains(&i) {
            println!("pushing field {:?}", col_names[i]);
            field_col_itrs.push(p.iter());
        } else if i == time_col {
            println!("pushing time {:?}", col_names[i]);
            time_col_itr = Some(p.iter());
        } else {
            unreachable!("missing indexes covering this column");
        }
    }
    let mut time_col_itr = time_col_itr.unwrap();

    while rows < total_rows {
        if rows > 0 {
            println!();
        }

        // Write the measurement.
        print!("{},", measurement);

        // Write the tags out.
        let tags = tag_col_itrs.len();
        for (i, itr) in &mut tag_col_itrs.iter_mut().enumerate() {
            let name = &col_names[i];

            match itr {
                packers::packers::PackersIterator::Float(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Integer(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Bytes(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v.as_utf8().unwrap()),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::String(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v.as_str()),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Boolean(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v),
                            None => print!("NULL"),
                        }
                    }
                }
            }

            if i < tags - 1 {
                print!(",");
            } else {
                print!(" ");
            }
        }

        // Write the fields out.
        let fields = field_col_itrs.len();
        for (i, itr) in &mut field_col_itrs.iter_mut().enumerate() {
            let name = &col_names[i + tag_col_itrs.len()];

            match itr {
                packers::packers::PackersIterator::Float(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}", name, v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Integer(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{}={}i", name, v),
                            None => print!("NULL"),
                        }
                    }
                }
                _ => panic!("not supported at the moment"),
            }

            if i < fields - 1 {
                print!(",");
            } else {
                print!(" ");
            }
        }

        // write timestamp out.
        match &mut time_col_itr {
            packers::packers::PackersIterator::Integer(itr) => {
                if let Some(v) = itr.next() {
                    match v {
                        Some(v) => print!("{:?}", v),
                        None => panic!("nope"),
                    }
                }
            }
            _ => panic!("not supported at the moment"),
        }

        rows += 1;
    }
}

fn save_record_batch(rb: &RecordBatch) {
    let file = File::create("/Users/edd/tracing_100m.arrow").unwrap();
    let mut writer = writer::StreamWriter::try_new(file, &rb.schema()).unwrap();
    writer.write(&rb).unwrap();
    writer.finish().unwrap();
}

fn load_record_batch(name: &str) -> Vec<RecordBatch> {
    let now = std::time::Instant::now();
    let file = File::open(name).unwrap();
    let reader = reader::StreamReader::try_new(file).unwrap();
    let rbs = reader.map(|r| r.unwrap()).collect::<Vec<_>>();
    println!("Loading record batches from arrow took {:?}", now.elapsed());

    rbs
}
