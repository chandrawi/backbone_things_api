use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_server::proto::resource::group::group_service_client::GroupServiceClient;
use bbthings_grpc_server::proto::resource::group::{
    GroupModelSchema, GroupDeviceSchema, GroupId, GroupIds, GroupName, GroupOption, GroupCategory, GroupUpdate,
    GroupModel, GroupDevice
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const GROUP_NOT_FOUND: &str = "requested group not found";

pub(crate) async fn read_group_model(resource: &Resource, id: Uuid)
    -> Result<GroupModelSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_group_model(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(GROUP_NOT_FOUND))?)
}

pub(crate) async fn list_group_model_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<GroupModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_group_model_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_model_by_name(resource: &Resource, name: &str)
    -> Result<Vec<GroupModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupName {
        name: name.to_owned()
    });
    let response = client.list_group_model_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_model_by_category(resource: &Resource, category: &str)
    -> Result<Vec<GroupModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupCategory {
        category: category.to_owned()
    });
    let response = client.list_group_model_by_category(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_model_option(resource: &Resource, name: Option<&str>, category: Option<&str>)
    -> Result<Vec<GroupModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupOption {
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned())
    });
    let response = client.list_group_model_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_group_model(resource: &Resource, id: Uuid, name: &str, category: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupModelSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        category: category.to_owned(),
        description: description.to_owned(),
        model_ids: Vec::new()
    });
    let response = client.create_group_model(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_group_model(resource: &Resource, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_group_model(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_group_model(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    client.delete_group_model(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_group_model_member(resource: &Resource, id: Uuid, model_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupModel {
        id: id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec()
    });
    client.add_group_model_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_group_model_member(resource: &Resource, id: Uuid, model_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupModel {
        id: id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec()
    });
    client.remove_group_model_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_group_device(resource: &Resource, id: Uuid)
    -> Result<GroupDeviceSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_group_device(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(GROUP_NOT_FOUND))?)
}

pub(crate) async fn list_group_device_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_group_device_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_device_by_name(resource: &Resource, name: &str)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupName {
        name: name.to_owned()
    });
    let response = client.list_group_device_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_device_by_category(resource: &Resource, category: &str)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupCategory {
        category: category.to_owned()
    });
    let response = client.list_group_device_by_category(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_device_option(resource: &Resource, name: Option<&str>, category: Option<&str>)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupOption {
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned())
    });
    let response = client.list_group_device_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_group_device(resource: &Resource, id: Uuid, name: &str, category: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDeviceSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        category: category.to_owned(),
        description: description.to_owned(),
        device_ids: Vec::new()
    });
    let response = client.create_group_device(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_group_device(resource: &Resource, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_group_device(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_group_device(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    client.delete_group_device(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_group_device_member(resource: &Resource, id: Uuid, device_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDevice {
        id: id.as_bytes().to_vec(),
        device_id: device_id.as_bytes().to_vec()
    });
    client.add_group_device_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_group_device_member(resource: &Resource, id: Uuid, device_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDevice {
        id: id.as_bytes().to_vec(),
        device_id: device_id.as_bytes().to_vec()
    });
    client.remove_group_device_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_group_gateway(resource: &Resource, id: Uuid)
    -> Result<GroupDeviceSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_group_gateway(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(GROUP_NOT_FOUND))?)
}

pub(crate) async fn list_group_gateway_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_group_gateway_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_gateway_by_name(resource: &Resource, name: &str)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupName {
        name: name.to_owned()
    });
    let response = client.list_group_gateway_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_gateway_by_category(resource: &Resource, category: &str)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupCategory {
        category: category.to_owned()
    });
    let response = client.list_group_gateway_by_category(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_group_gateway_option(resource: &Resource, name: Option<&str>, category: Option<&str>)
    -> Result<Vec<GroupDeviceSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupOption {
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned())
    });
    let response = client.list_group_gateway_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_group_gateway(resource: &Resource, id: Uuid, name: &str, category: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDeviceSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        category: category.to_owned(),
        description: description.to_owned(),
        device_ids: Vec::new()
    });
    let response = client.create_group_gateway(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_group_gateway(resource: &Resource, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_group_gateway(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_group_gateway(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupId {
        id: id.as_bytes().to_vec()
    });
    client.delete_group_gateway(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_group_gateway_member(resource: &Resource, id: Uuid, gateway_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDevice {
        id: id.as_bytes().to_vec(),
        device_id: gateway_id.as_bytes().to_vec()
    });
    client.add_group_gateway_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_group_gateway_member(resource: &Resource, id: Uuid, gateway_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        GroupServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(GroupDevice {
        id: id.as_bytes().to_vec(),
        device_id: gateway_id.as_bytes().to_vec()
    });
    client.remove_group_gateway_member(request)
        .await?;
    Ok(())
}
