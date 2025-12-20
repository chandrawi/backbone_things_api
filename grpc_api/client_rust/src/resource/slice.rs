use tonic::{Request, Status};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use bbthings_grpc_server::proto::resource::slice::slice_service_client::SliceServiceClient;
use bbthings_grpc_server::proto::resource::slice::{
    SliceSchema, SliceId, SliceIds, SliceTime, SliceRange, SliceNameTime, SliceNameRange, SliceUpdate, SliceOption,
    SliceGroupTime, SliceGroupRange, SliceGroupOption,
    SliceSetSchema, SliceSetTime, SliceSetRange, SliceSetOption
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const SLICE_NOT_FOUND: &str = "requested slice not found";

pub(crate) async fn read_slice(resource: &Resource, id: i32)
    -> Result<SliceSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceId {
        id
    });
    let response = client.read_slice(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(SLICE_NOT_FOUND))?)
}

pub(crate) async fn list_slice_by_ids(resource: &Resource, ids: &[i32])
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceIds {
        ids: ids.to_vec()
    });
    let response = client.list_slice_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_by_time(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceTime {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros()
    });
    let response = client.list_slice_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_by_range(resource: &Resource, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceRange {
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros()
    });
    let response = client.list_slice_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_by_name_time(resource: &Resource, name: &str, timestamp: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceNameTime {
        name: name.to_owned(),
        timestamp: timestamp.timestamp_micros()
    });
    let response = client.list_slice_by_name_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_by_name_range(resource: &Resource, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceNameRange {
        name: name.to_owned(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros()
    });
    let response = client.list_slice_by_name_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_option(resource: &Resource, device_id: Option<Uuid>, model_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceOption {
        device_id: device_id.map(|id| id.as_bytes().to_vec()),
        model_id: model_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned()),
        begin: begin_or_timestamp.map(|t| t.timestamp_micros()),
        end: end.map(|t| t.timestamp_micros())
    });
    let response = client.list_slice_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_group_by_time(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceGroupTime {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        timestamp: timestamp.timestamp_micros()
    });
    let response = client.list_slice_group_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_group_by_range(resource: &Resource, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceGroupRange {
        device_ids: device_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros()
    });
    let response = client.list_slice_group_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_group_option(resource: &Resource, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
    -> Result<Vec<SliceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let mut id_flag = 0;
    if let Some(_) = device_ids { id_flag += 1; }
    if let Some(_) = model_ids { id_flag += 2; }
    let request = Request::new(SliceGroupOption {
        device_ids: device_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        model_ids: model_ids.unwrap_or_default().into_iter().map(|id| id.as_bytes().to_vec()).collect(),
        name: name.map(|s| s.to_owned()),
        begin: begin_or_timestamp.map(|t| t.timestamp_micros()),
        end: end.map(|t| t.timestamp_micros()),
        id_flag
    });
    let response = client.list_slice_group_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_slice(resource: &Resource, device_id: Uuid, model_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: &str)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceSchema {
        id: 0,
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        timestamp_begin: timestamp_begin.timestamp_micros(),
        timestamp_end: timestamp_end.timestamp_micros(),
        name: name.to_owned(),
        description: description.to_owned()
    });
    let response = client.create_slice(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn update_slice(resource: &Resource, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceUpdate {
        id,
        timestamp_begin: timestamp_begin.map(|t| t.timestamp_micros()),
        timestamp_end: timestamp_end.map(|t| t.timestamp_micros()),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_slice(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_slice(resource: &Resource, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceId {
        id
    });
    client.delete_slice(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_slice_set(resource: &Resource, id: i32)
    -> Result<SliceSetSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceId {
        id
    });
    let response = client.read_slice_set(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(SLICE_NOT_FOUND))?)
}

pub(crate) async fn list_slice_set_by_ids(resource: &Resource, ids: &[i32])
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceIds {
        ids: ids.to_vec()
    });
    let response = client.list_slice_set_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_set_by_time(resource: &Resource, set_id: Uuid, timestamp: DateTime<Utc>)
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceSetTime {
        set_id: set_id.as_bytes().to_vec(),
        timestamp: timestamp.timestamp_micros()
    });
    let response = client.list_slice_set_by_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_set_by_range(resource: &Resource, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceSetRange {
        set_id: set_id.as_bytes().to_vec(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros()
    });
    let response = client.list_slice_set_by_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_set_by_name_time(resource: &Resource, name: &str, timestamp: DateTime<Utc>)
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceNameTime {
        name: name.to_owned(),
        timestamp: timestamp.timestamp_micros()
    });
    let response = client.list_slice_set_by_name_time(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_set_by_name_range(resource: &Resource, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceNameRange {
        name: name.to_owned(),
        begin: begin.timestamp_micros(),
        end: end.timestamp_micros()
    });
    let response = client.list_slice_set_by_name_range(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_slice_set_option(resource: &Resource, set_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
    -> Result<Vec<SliceSetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceSetOption {
        set_id: set_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned()),
        begin: begin_or_timestamp.map(|t| t.timestamp_micros()),
        end: end.map(|t| t.timestamp_micros())
    });
    let response = client.list_slice_set_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_slice_set(resource: &Resource, set_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: &str)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceSetSchema {
        id: 0,
        set_id: set_id.as_bytes().to_vec(),
        timestamp_begin: timestamp_begin.timestamp_micros(),
        timestamp_end: timestamp_end.timestamp_micros(),
        name: name.to_owned(),
        description: description.to_owned()
    });
    let response = client.create_slice_set(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn update_slice_set(resource: &Resource, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceUpdate {
        id,
        timestamp_begin: timestamp_begin.map(|t| t.timestamp_micros()),
        timestamp_end: timestamp_end.map(|t| t.timestamp_micros()),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_slice_set(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_slice_set(resource: &Resource, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SliceServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SliceId {
        id
    });
    client.delete_slice_set(request)
        .await?;
    Ok(())
}
