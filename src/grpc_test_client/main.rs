// This is a simple command line program that sends queries via gRPC and prints the results

use generated_types::{
    node,
    Node,
    aggregate::AggregateType,
    //i_ox_testing_client,
    read_response::frame,
    storage_client,
    Aggregate as RPCAggregate,
    //Duration as RPCDuration,
    ReadSource,
    //Window as RPCWindow,
//     i_ox_testing_server::{IOxTesting, IOxTestingServer},
//     storage_server::{Storage, StorageServer},
//     CapabilitiesResponse, Capability, Int64ValuesResponse, MeasurementFieldsRequest,
//     MeasurementFieldsResponse, MeasurementNamesRequest, MeasurementTagKeysRequest,
    //     MeasurementTagValuesRequest,
    Predicate,
    //ReadFilterRequest,
ReadGroupRequest,
//ReadResponse,
//     ReadSeriesCardinalityRequest, ReadWindowAggregateRequest, StringValuesResponse, TagKeysRequest,
    //     TagValuesRequest, TestErrorRequest, TestErrorResponse,
    TimestampRange,
};



type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;






use futures::TryStreamExt;

//use generated_types;
use tokio;

#[tokio::main]
async fn main() {

    // this is the port that the gRPC storage service listens to
    let url = "http://127.0.0.1:8086";

    let mut client = match StorageClient::connect(url).await {
        Ok(client) => client,
        Err(e) => {
            println!("Error connecting: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

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
            r#type: AggregateType::Sum as i32,
        }),
        hints: 0,
    };

    let results = match read_group(&mut client, request).await {
        Ok(results) =>  results,
        Err(e) => {
            println!("Error running read group: {}", e);
            println!("   {:?}", e);
            return ();
        }
    };

    println!("Results:\n{}", results.join("\n"));
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
        type_url: "/TODO".to_string(),
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

    let data_frames: Vec<frame::Data> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    let s = format!("{} group frames", data_frames.len());

    Ok(vec![s])
}
