use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_server::proto::resource::set::set_service_client::SetServiceClient;
use bbthings_grpc_server::proto::resource::set::{
    SetSchema, SetId, SetIds, SetName, SetOption, SetUpdate, SetMemberRequest, SetMemberSwap,
    SetTemplateSchema, SetTemplateId, SetTemplateIds, SetTemplateName, SetTemplateOption, SetTemplateUpdate, 
    SetTemplateMemberRequest, SetTemplateMemberSwap
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const SET_NOT_FOUND: &str = "requested set not found";

pub(crate) async fn read_set(resource: &Resource, id: Uuid)
    -> Result<SetSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_set(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(SET_NOT_FOUND))?)
}

pub(crate) async fn list_set_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<SetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_set_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_set_by_template(resource: &Resource, template_id: Uuid)
    -> Result<Vec<SetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateId {
        id: template_id.as_bytes().to_vec()
    });
    let response = client.list_set_by_template(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_set_by_name(resource: &Resource, name: &str)
    -> Result<Vec<SetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetName {
        name: name.to_owned()
    });
    let response = client.list_set_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_set_option(resource: &Resource, template_id: Option<Uuid>, name: Option<&str>)
    -> Result<Vec<SetSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetOption {
        template_id: template_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned())
    });
    let response = client.list_set_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_set(resource: &Resource, id: Uuid, template_id: Uuid, name: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetSchema {
        id: id.as_bytes().to_vec(),
        template_id: template_id.as_bytes().to_vec(),
        name: name.to_owned(),
        description: description.to_owned(),
        members: Vec::new()
    });
    let response = client.create_set(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_set(resource: &Resource, id: Uuid, template_id: Option<Uuid>, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetUpdate {
        id: id.as_bytes().to_vec(),
        template_id: template_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_set(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_set(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetId {
        id: id.as_bytes().to_vec()
    });
    client.delete_set(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_set_member(resource: &Resource, id: Uuid, device_id: Uuid, model_id: Uuid, data_index: &[u8])
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetMemberRequest {
        id: id.as_bytes().to_vec(),
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        data_index: data_index.to_owned()
    });
    client.add_set_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_set_member(resource: &Resource, id: Uuid, device_id: Uuid, model_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetMemberRequest {
        id: id.as_bytes().to_vec(),
        device_id: device_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        data_index: Vec::new()
    });
    client.remove_set_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn swap_set_member(resource: &Resource, id: Uuid, device_id_1: Uuid, model_id_1: Uuid, device_id_2: Uuid, model_id_2: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetMemberSwap {
        id: id.as_bytes().to_vec(),
        device_id_1: device_id_1.as_bytes().to_vec(),
        model_id_1: model_id_1.as_bytes().to_vec(),
        device_id_2: device_id_2.as_bytes().to_vec(),
        model_id_2: model_id_2.as_bytes().to_vec(),
    });
    client.swap_set_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_set_template(resource: &Resource, id: Uuid)
    -> Result<SetTemplateSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_set_template(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(SET_NOT_FOUND))?)
}

pub(crate) async fn list_set_template_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<SetTemplateSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_set_template_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_set_template_by_name(resource: &Resource, name: &str)
    -> Result<Vec<SetTemplateSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateName {
        name: name.to_owned()
    });
    let response = client.list_set_template_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_set_template_option(resource: &Resource, name: Option<&str>)
    -> Result<Vec<SetTemplateSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateOption {
        name: name.map(|s| s.to_owned())
    });
    let response = client.list_set_template_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_set_template(resource: &Resource, id: Uuid, name: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        description: description.to_owned(),
        members: Vec::new()
    });
    let response = client.create_set_template(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_set_template(resource: &Resource, id: Uuid, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_set_template(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_set_template(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateId {
        id: id.as_bytes().to_vec()
    });
    client.delete_set_template(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_set_template_member(resource: &Resource, id: Uuid, type_id: Uuid, model_id: Uuid, data_index: &[u8])
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateMemberRequest {
        id: id.as_bytes().to_vec(),
        type_id: type_id.as_bytes().to_vec(),
        model_id: model_id.as_bytes().to_vec(),
        data_index: data_index.to_owned(),
        template_index: 0
    });
    client.add_set_template_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_set_template_member(resource: &Resource, id: Uuid, index: usize)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateMemberRequest {
        id: id.as_bytes().to_vec(),
        type_id: Uuid::nil().as_bytes().to_vec(),
        model_id: Uuid::nil().as_bytes().to_vec(),
        data_index: Vec::new(),
        template_index: index as i32
    });
    client.remove_set_template_member(request)
        .await?;
    Ok(())
}

pub(crate) async fn swap_set_template_member(resource: &Resource, id: Uuid, index_1: usize, index_2: usize)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        SetServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(SetTemplateMemberSwap {
        id: id.as_bytes().to_vec(),
        template_index_1: index_1 as i32,
        template_index_2: index_2 as i32
    });
    client.swap_set_template_member(request)
        .await?;
    Ok(())
}
