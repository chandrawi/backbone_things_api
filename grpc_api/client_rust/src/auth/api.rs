use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_proto::auth::api::api_service_client::ApiServiceClient;
use bbthings_grpc_proto::auth::api::{
    ApiId, ApiIds, ApiName, ApiCategory, ApiOption, ApiSchema, ApiUpdate, 
    ProcedureId, ProcedureIds, ProcedureName, ProcedureOption, ProcedureSchema, ProcedureUpdate
};
use crate::auth::Auth;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const API_NOT_FOUND: &str = "requested api not found";
const PROC_NOT_FOUND: &str = "requested procedure not found";

pub(crate) async fn read_api(auth: &Auth, id: Uuid)
    -> Result<ApiSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_api(request).await?.into_inner();
    Ok(response.result.ok_or(Status::not_found(API_NOT_FOUND))?)
}

pub(crate) async fn read_api_by_name(auth: &Auth, name: &str)
    -> Result<ApiSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiName {
        name: name.to_owned()
    });
    let response = client.read_api_by_name(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(API_NOT_FOUND))?)
}

pub(crate) async fn list_api_by_ids(auth: &Auth, ids: &[Uuid])
    -> Result<Vec<ApiSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_api_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_api_by_name(auth: &Auth, name: &str)
    -> Result<Vec<ApiSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiName {
        name: name.to_owned()
    });
    let response = client.list_api_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_api_by_category(auth: &Auth, category: &str)
    -> Result<Vec<ApiSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiCategory {
        category: category.to_owned()
    });
    let response = client.list_api_by_category(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_api_option(auth: &Auth, name: Option<&str>, category: Option<&str>)
    -> Result<Vec<ApiSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiOption {
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned())
    });
    let response = client.list_api_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_api(auth: &Auth, id: Uuid, name: &str, address: &str, category: &str, description: &str, password: &str, access_key: &[u8])
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        address: address.to_owned(),
        category: category.to_owned(),
        description: description.to_owned(),
        password: password.to_owned(),
        access_key: access_key.to_vec(),
        procedures: Vec::new()
    });
    let response = client.create_api(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_api(auth: &Auth, id: Uuid, name: Option<&str>, address: Option<&str>, category: Option<&str>, description: Option<&str>, password: Option<&str>, access_key: Option<&[u8]>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        address: address.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned()),
        password: password.map(|s| s.to_owned()),
        access_key: access_key.map(|v| v.to_vec())
    });
    client.update_api(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_api(auth: &Auth, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiId {
        id: id.as_bytes().to_vec()
    });
    client.delete_api(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_procedure(auth: &Auth, id: Uuid)
    -> Result<ProcedureSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_procedure(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(PROC_NOT_FOUND))?)
}

pub(crate) async fn read_procedure_by_name(auth: &Auth, api_id: Uuid, name: &str)
    -> Result<ProcedureSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureName {
        api_id: api_id.as_bytes().to_vec(),
        name: name.to_owned()
    });
    let response = client.read_procedure_by_name(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(PROC_NOT_FOUND))?)
}

pub(crate) async fn list_procedure_by_ids(auth: &Auth, ids: &[Uuid])
    -> Result<Vec<ProcedureSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_procedure_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_procedure_by_api(auth: &Auth, api_id: Uuid)
    -> Result<Vec<ProcedureSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiId {
        id: api_id.as_bytes().to_vec()
    });
    let response = client.list_procedure_by_api(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_procedure_by_name(auth: &Auth, name: &str)
    -> Result<Vec<ProcedureSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureName {
        api_id: Uuid::nil().as_bytes().to_vec(),
        name: name.to_owned()
    });
    let response = client.list_procedure_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_procedure_option(auth: &Auth, api_id: Option<Uuid>, name: Option<&str>)
    -> Result<Vec<ProcedureSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureOption {
        api_id: api_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned())
    });
    let response = client.list_procedure_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_procedure(auth: &Auth, id: Uuid, api_id: Uuid, name: &str, description: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureSchema {
        id: id.as_bytes().to_vec(),
        api_id: api_id.as_bytes().to_vec(),
        name: name.to_owned(),
        description: description.to_owned(),
        roles: Vec::new()
    });
    let response = client.create_procedure(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_procedure(auth: &Auth, id: Uuid, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned())
    });
    client.update_procedure(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_procedure(auth: &Auth, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ApiServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProcedureId {
        id: id.as_bytes().to_vec()
    });
    client.delete_procedure(request)
        .await?;
    Ok(())
}
