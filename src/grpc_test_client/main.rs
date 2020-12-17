// This is a simple command line program that sends queries via gRPC and prints the results
// run with
//   cargo run --bin grpc_test_client
//
// Run with much logness:
//   RUST_LOG=trace cargo run --bin grpc_test_client


use generated_types::{
    node,
    Node,
    aggregate::AggregateType,
    //i_ox_testing_client,
    //read_response::frame,
    storage_client,
    Aggregate as RPCAggregate,
    //Duration as RPCDuration,
    ReadSource,
    //Window as RPCWindow,
//     i_ox_testing_server::{IOxTesting, IOxTestingServer},
//     storage_server::{Storage, StorageServer},
    CapabilitiesResponse,
    //Capability, Int64ValuesResponse, MeasurementFieldsRequest,
//     MeasurementFieldsResponse, MeasurementNamesRequest, MeasurementTagKeysRequest,
    //     MeasurementTagValuesRequest,
    Predicate,
    //ReadFilterRequest,
ReadGroupRequest,
//ReadResponse,
//     ReadSeriesCardinalityRequest, ReadWindowAggregateRequest, StringValuesResponse, TagKeysRequest,
    //     TagValuesRequest, TestErrorRequest, TestErrorResponse,
    TimestampRange,
    read_response::{
        frame::Data, BooleanPointsFrame, FloatPointsFrame, Frame, GroupFrame,
        IntegerPointsFrame, SeriesFrame, StringPointsFrame,
    },
    Tag,
};
use tracing_subscriber::EnvFilter;



type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

use std::collections::HashMap;

use futures::TryStreamExt;

//use generated_types;
use tokio;

#[tokio::main]
async fn main() {
    use tracing_subscriber::prelude::*;

    // Setup logging...
    let logger = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    tracing_subscriber::registry()
        //.with(opentelemetry)
        .with(EnvFilter::from_default_env())
        .with(logger)
        .init();


    // 8082 is the port that the gRPC storage service listens to
    let url = "http://127.0.0.1:8082";

    let mut client = match StorageClient::connect(url).await {
        Ok(client) => client,
        Err(e) => {
            println!("Error connecting: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Sucessfully connected to server {}. Client: {:?}", url, client);

    println!("Checking capabilities...");
    let caps = match capabilities(&mut client).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running capabilities: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };
    println!("Capabilities:\n{:#?}", caps);


    let org_id  = 0x26f7e5a4b7be365b;
    let bucket_id  = 0x917b97a92e883afc;
    let partition_id = 0;

    let read_source = make_read_source(org_id, bucket_id, partition_id);

    let group = generated_types::read_group_request::Group::By as i32;

    let range = TimestampRange {
        start: 1608046337935448000,
        end: 1608219137935448000,
    };

    let request = ReadGroupRequest {
        read_source: Some(read_source),
        range: Some(range),
        predicate: make_measurement_predicate("h2o"),
        //group_keys: vec!["tag1".into()],
        group_keys: vec![],
        group,
        aggregate: Some(RPCAggregate {
            r#type: AggregateType::First as i32,
        }),
        hints: 0,
    };

    println!("------ first -------");
    let results = match read_group(&mut client, make_request_from_template(request.clone(), AggregateType::First, &[])).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running read group: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Results for first:\n{}", results.join("\n"));


    println!("------ last -------");
    let results = match read_group(&mut client, make_request_from_template(request.clone(), AggregateType::Last, &[])).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running read group: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Results for last:\n{}", results.join("\n"));


    println!("------ min -------");
    let results = match read_group(&mut client, make_request_from_template(request.clone(), AggregateType::Min, &[])).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running read group: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Results for min:\n{}", results.join("\n"));

    println!("------ max -------");
    let results = match read_group(&mut client, make_request_from_template(request.clone(), AggregateType::Max, &[])).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running read group: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Results for max:\n{}", results.join("\n"));

}

// Takes the request and replaces the aggregate and group keys list
fn make_request_from_template(request: ReadGroupRequest, agg: AggregateType, group_keys: &[&str]) -> ReadGroupRequest {
    let group_keys = group_keys.iter().map(|s|s.to_string())
        .collect::<Vec<_>>();

    ReadGroupRequest {
        aggregate: Some(RPCAggregate {
            r#type: agg as i32,
        }),
        group_keys,
        ..request
    }


}


/// Create a ReadSource suitable for constructing messages
fn make_read_source(org_id: u64, bucket_id: u64, partition_id: u64) -> prost_types::Any {
    use prost::Message;

    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };
    let mut d = Vec::new();
            read_source
        .encode(&mut d)
        .expect("encoded read source appropriately");
    prost_types::Any {
        type_url: "/com.github.influxdata.idpe.storage.read.ReadSource".to_string(),
        value: d,
    }
}



/// return a predicate like
///
/// _m=measurement_name
fn make_measurement_predicate(measurement_name: &str) -> Option<Predicate> {
    use node::{Comparison, Type, Value};
    let root = Node {
        node_type: Type::ComparisonExpression as i32,
        value: Some(Value::Comparison(Comparison::Equal as i32)),
        children: vec![
            Node {
                node_type: Type::TagRef as i32,
                value: Some(Value::TagRefValue([0].to_vec())),
                children: vec![],
            },
            Node {
                node_type: Type::Literal as i32,
                value: Some(Value::StringValue(measurement_name.into())),
                children: vec![],
            },
        ],
    };
    Some(Predicate { root: Some(root) })
}



/// return the capabilities of the server as a hash map
async fn capabilities(client: &mut StorageClient) -> Result<HashMap<String, Vec<String>>, tonic::Status> {
    let response = client.capabilities(()).await?.into_inner();

    let CapabilitiesResponse { caps } = response;

    // unwrap the Vec of Strings inside each `Capability`
    let caps = caps
        .into_iter()
        .map(|(name, capability)| (name, capability.features))
        .collect();

    Ok(caps)
}


/// Make a request to query::query_groups and do the
/// required async dance to flatten the resulting stream
async fn read_group(
    client: &mut StorageClient,
    request: ReadGroupRequest,
) -> Result<Vec<String>, tonic::Status> {
    let responses: Vec<_> = client
        .read_group(request)
        .await?
        .into_inner()
        .try_collect()
        .await?;

    let data_frames= responses
        .into_iter()
        .flat_map(|r| r.frames)
        .map(|frame| dump_frame(&frame))
        .collect();

    Ok(data_frames)
}


fn dump_frame(frame: &Frame) -> String {
    let data = &frame.data;
    match data {
        Some(Data::Series(SeriesFrame { tags, data_type })) => format!(
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => format!(
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => format!(
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => format!(
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => format!(
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => format!(
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals),
        ),
        None => "<NO data field>".into(),
        _ => ":thinking_face: unknown frame type".into(),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{}", item))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_u8_vec(encoded_strings: &[Vec<u8>]) -> String {
    encoded_strings
        .iter()
        .map(|b| String::from_utf8_lossy(b))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&tag.key),
                String::from_utf8_lossy(&tag.value),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
