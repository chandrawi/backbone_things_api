use tonic::{Request, Status};
use chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::common::type_value::{DataValue, ArrayDataValue};
use bbthings_database::common::tag as Tag;
use bbthings_grpc_proto::resource::data::data_service_client::DataServiceClient;
use bbthings_grpc_proto::resource::data::{
    DataSchema, DataMultipleSchema, DataTime, DataEarlier, DataLater, DataRange, DataNumber, 
    DataGroupTime, DataGroupEarlier, DataGroupLater, DataGroupRange, DataGroupNumber,
    DataSetSchema, DataSetTime, DataSetEarlier, DataSetLater, DataSetRange
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const DATA_NOT_FOUND: &str = "requested data not found";
pub(crate) const EMPTY_LENGTH_UNMATCH: &str = "One or more input array arguments are empty or doesn't have the same length";

pub(crate) async fn read_data(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DataSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.read_data(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(DATA_NOT_FOUND))?)
}

pub(crate) async fn list_data_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_by_number_before(resource: &Resource, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataNumber {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: before.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_number_before(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_by_number_after(resource: &Resource, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataNumber {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: after.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_by_number_after(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_time(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_number_before(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupNumber {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: before.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_number_before(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_group_by_number_after(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<DataSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupNumber {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: after.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_by_number_after(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn read_data_set(resource: &Resource, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DataSetSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSetTime {
        set_id: set_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.read_data_set(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(DATA_NOT_FOUND))?)
}

pub(crate) async fn list_data_set_by_time(resource: &Resource, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSetTime {
        set_id: set_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_set_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_set_by_earlier(resource: &Resource, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSetEarlier {
        set_id: set_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_set_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_set_by_later(resource: &Resource, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSetLater {
        set_id: set_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_set_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_data_set_by_range(resource: &Resource, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DataSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSetRange {
        set_id: set_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_set_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_data(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataSchema {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        data_bytes: ArrayDataValue::from_vec(data).to_bytes(),
        data_type: ArrayDataValue::from_vec(data).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tag.unwrap_or(Tag::DEFAULT) as i32
    });
    client.create_data(request)
        .await?;
    Ok(())
}

pub(crate) async fn create_data_multiple(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let number = device_ids.len();
    let tags = match tags {
        Some(value) => value.to_vec(),
        None => (0..number).map(|_| Tag::DEFAULT).collect()
    };
    let numbers = vec![model_ids.len(), timestamps.len(), data.len(), tags.len()];
    if number == 0 || numbers.into_iter().any(|n| n != number) {
        return Err(Status::invalid_argument(EMPTY_LENGTH_UNMATCH.to_string()))
    } 
    let schemas = (0..number).into_iter().map(|i| DataSchema {
        device_id: device_ids[i].as_bytes().to_vec(),
        model_id: model_ids[i].as_bytes().to_vec(),
        timestamp: timestamps[i].timestamp_micros(),
        data_bytes: ArrayDataValue::from_vec(data[i]).to_bytes(),
        data_type: ArrayDataValue::from_vec(data[i]).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tags[i] as i32
    }).collect();
    let request = Request::new(DataMultipleSchema { schemas });
    client.create_data_multiple(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_data(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    client.delete_data(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_data_timestamp(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DateTime<Utc>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.read_data_timestamp(request)
        .await?
        .into_inner();
    Ok(Utc.timestamp_nanos(response.timestamp * 1000))
}

pub(crate) async fn list_data_timestamp_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_timestamp_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_data_timestamp_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_timestamp_by_later(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_data_timestamp_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_timestamp_by_range(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn read_data_group_timestamp(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DateTime<Utc>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.read_data_group_timestamp(request)
        .await?
        .into_inner();
    Ok(Utc.timestamp_nanos(response.timestamp * 1000))
}

pub(crate) async fn list_data_group_timestamp_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_timestamp_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_data_group_timestamp_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_timestamp_by_later(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_data_group_timestamp_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.list_data_group_timestamp_by_range(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn count_data(resource: &Resource, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: 0,
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_by_later(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_by_range(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_group(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: 0,
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_group(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_group_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_group_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_group_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_group_by_later(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_data_group_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        DataServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(DataGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|t| t as i32)
    });
    let response = client.count_data_group_by_range(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}
