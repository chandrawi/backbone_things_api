use tonic::{Request, Status};
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;
use bbthings_database::common::type_value::{DataValue, ArrayDataValue};
use bbthings_database::common::tag as Tag;
use bbthings_grpc_proto::resource::buffer::buffer_service_client::BufferServiceClient;
use bbthings_grpc_proto::resource::buffer::{
    BufferSchema, BufferMultipleSchema, BufferId, BufferIds, BufferTime, BufferEarlier, BufferLater, BufferRange, BufferNumber, 
    BufferSelector, BuffersSelector, BufferUpdate, BufferUpdateTime, 
    BufferGroupTime, BufferGroupEarlier, BufferGroupLater, BufferGroupRange, BufferGroupNumber, BufferGroupSelector, BuffersGroupSelector,
    BufferSetSchema, BufferSetTime, BufferSetEarlier, BufferSetLater, BufferSetRange
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const BUFFER_NOT_FOUND: &str = "requested buffer not found";
use super::data::EMPTY_LENGTH_UNMATCH;

pub(crate) async fn read_buffer(resource: &Resource, id: i32)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferId {
        id
    });
    let response = client.read_buffer(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn read_buffer_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_by_time(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn list_buffer_by_ids(resource: &Resource, ids: &[i32])
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferIds {
        ids: ids.to_vec()
    });
    let response = client.list_buffer_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_number_before(resource: &Resource, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferNumber {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: before.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_number_before(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_by_number_after(resource: &Resource, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferNumber {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: after.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_by_number_after(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn read_buffer_first(resource: &Resource, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_first(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn read_buffer_last(resource: &Resource, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_last(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn list_buffer_first(resource: &Resource, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_first(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_first_offset(resource: &Resource, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: offset as u32
    });
    let response = client.list_buffer_first_offset(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_last(resource: &Resource, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_last(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_last_offset(resource: &Resource, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: offset as u32
    });
    let response = client.list_buffer_last_offset(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_time(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_number_before(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupNumber {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: before.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_number_before(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_by_number_after(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupNumber {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: after.timestamp_micros(),
        number: number as u32,
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_by_number_after(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn read_buffer_group_first(resource: &Resource, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_group_first(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn read_buffer_group_last(resource: &Resource, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<BufferSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_group_last(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn list_buffer_group_first(resource: &Resource, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_group_first(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_first_offset(resource: &Resource, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: offset as u32
    });
    let response = client.list_buffer_group_first_offset(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_last(resource: &Resource, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_group_last(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_group_last_offset(resource: &Resource, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<BufferSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: offset as u32
    });
    let response = client.list_buffer_group_last_offset(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn read_buffer_set(resource: &Resource, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<BufferSetSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSetTime {
        set_id: set_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_set(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(BUFFER_NOT_FOUND))?)
}

pub(crate) async fn list_buffer_set_by_time(resource: &Resource, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSetTime {
        set_id: set_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_set_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_set_by_earlier(resource: &Resource, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSetEarlier {
        set_id: set_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_set_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_set_by_later(resource: &Resource, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSetLater {
        set_id: set_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_set_by_later(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_buffer_set_by_range(resource: &Resource, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<BufferSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSetRange {
        set_id: set_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_set_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_buffer(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferSchema {
        id: 0,
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        data_bytes: ArrayDataValue::from_vec(data).to_bytes(),
        data_type: ArrayDataValue::from_vec(data).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tag.unwrap_or(Tag::DEFAULT) as i32
    });
    let response = client.create_buffer(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn create_buffer_multiple(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
    -> Result<Vec<i32>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let number = device_ids.len();
    let tags = match tags {
        Some(value) => value.to_vec(),
        None => (0..number).map(|_| Tag::DEFAULT).collect()
    };
    let numbers = vec![model_ids.len(), timestamps.len(), data.len(), tags.len()];
    if number == 0 || numbers.into_iter().any(|n| n != number) {
        return Err(Status::invalid_argument(EMPTY_LENGTH_UNMATCH.to_string()))
    } 
    let schemas = (0..number).into_iter().map(|i| BufferSchema {
        id: 0,
        device_id: device_ids[i].as_bytes().to_vec(),
        model_id: model_ids[i].as_bytes().to_vec(),
        timestamp: timestamps[i].timestamp_micros(),
        data_bytes: ArrayDataValue::from_vec(data[i]).to_bytes(),
        data_type: ArrayDataValue::from_vec(data[i]).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tags[i] as i32
    }).collect();
    let request = Request::new(BufferMultipleSchema { schemas });
    let response = client.create_buffer_multiple(request)
        .await?
        .into_inner();
    Ok(response.ids)
}

pub(crate) async fn update_buffer(resource: &Resource, id: i32, data: Option<&[DataValue]>, tag: Option<i16>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferUpdate {
        id,
        data_bytes: data.as_deref().map(|v| ArrayDataValue::from_vec(v).to_bytes()),
        data_type: ArrayDataValue::from_vec(&data.unwrap_or_default()).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tag.map(|i| i as i32)
    });
    client.update_buffer(request)
        .await?;
    Ok(())
}

pub(crate) async fn update_buffer_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Option<&[DataValue]>, tag: Option<i16>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferUpdateTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        data_bytes: data.as_deref().map(|v| ArrayDataValue::from_vec(v).to_bytes()),
        data_type: ArrayDataValue::from_vec(&data.unwrap_or_default()).get_types().into_iter().map(|el| el.into()).collect(),
        tag: tag.map(|i| i as i32)
    });
    client.update_buffer_by_time(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_buffer(resource: &Resource, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferId {
        id
    });
    client.delete_buffer(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_buffer_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    client.delete_buffer_by_time(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_buffer_timestamp(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DateTime<Utc>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_timestamp(request)
        .await?
        .into_inner();
    Ok(Utc.timestamp_nanos(response.timestamp * 1000))
}

pub(crate) async fn list_buffer_timestamp_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_timestamp_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_timestamp_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_timestamp_by_later(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_timestamp_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_timestamp_by_range(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_timestamp_first(resource: &Resource, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_timestamp_first(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_timestamp_last(resource: &Resource, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
    BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersSelector {
        device_id: device_id.map(|x| x.as_bytes().to_vec()),
        model_id: model_id.map(|x| x.as_bytes().to_vec()),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_timestamp_last(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn read_buffer_group_timestamp(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
    -> Result<DateTime<Utc>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: timestamp.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.read_buffer_group_timestamp(request)
        .await?
        .into_inner();
    Ok(Utc.timestamp_nanos(response.timestamp * 1000))
}

pub(crate) async fn list_buffer_group_timestamp_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_timestamp_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_group_timestamp_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_timestamp_by_later(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_group_timestamp_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.list_buffer_group_timestamp_by_range(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_group_timestamp_first(resource: &Resource, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_group_timestamp_first(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn list_buffer_group_timestamp_last(resource: &Resource, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
    -> Result<Vec<DateTime<Utc>>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BuffersGroupSelector {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        tag: tag.map(|i| i as i32),
        number: number as u32,
        offset: 0
    });
    let response = client.list_buffer_group_timestamp_last(request)
        .await?
        .into_inner();
    Ok(response.timestamps.into_iter().map(|t| Utc.timestamp_nanos(t * 1000)).collect())
}

pub(crate) async fn count_buffer(resource: &Resource, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: 0,
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_by_earlier(resource: &Resource, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferEarlier {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_by_later(resource: &Resource, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferLater {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_by_later(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_by_range(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_group(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: 0,
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_group(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_group_by_earlier(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupEarlier {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        earlier: earlier.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_group_by_earlier(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_group_by_later(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupLater {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        later: later.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_group_by_later(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}

pub(crate) async fn count_buffer_group_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
    -> Result<usize, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        BufferServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(BufferGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros(),
        tag: tag.map(|i| i as i32)
    });
    let response = client.count_buffer_group_by_range(request)
        .await?
        .into_inner();
    Ok(response.count as usize)
}
