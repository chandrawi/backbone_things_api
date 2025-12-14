use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_server::proto::auth::role::role_service_client::RoleServiceClient;
use bbthings_grpc_server::proto::auth::role::{
    RoleId, RoleIds, RoleName, ApiId, UserId, RoleOption, RoleSchema, RoleUpdate, RoleAccess
};
use crate::auth::Auth;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const ROLE_NOT_FOUND: &str = "requested role not found";

pub(crate) async fn read_role(auth: &Auth, id: Uuid)
    -> Result<RoleSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_role(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(ROLE_NOT_FOUND))?)
}

pub(crate) async fn read_role_by_name(auth: &Auth, api_id: Uuid, name: &str)
    -> Result<RoleSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleName {
        api_id: api_id.as_bytes().to_vec(),
        name: name.to_owned()
    });
    let response = client.read_role_by_name(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(ROLE_NOT_FOUND))?)
}

pub(crate) async fn list_role_by_ids(auth: &Auth, ids: &[Uuid])
    -> Result<Vec<RoleSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_role_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_role_by_api(auth: &Auth, api_id: Uuid)
    -> Result<Vec<RoleSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiId {
        api_id: api_id.as_bytes().to_vec()
    });
    let response = client.list_role_by_api(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_role_by_user(auth: &Auth, user_id: Uuid)
    -> Result<Vec<RoleSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserId {
        user_id: user_id.as_bytes().to_vec()
    });
    let response = client.list_role_by_user(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_role_by_name(auth: &Auth, name: &str)
    -> Result<Vec<RoleSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleName {
        api_id: Uuid::nil().as_bytes().to_vec(),
        name: name.to_owned()
    });
    let response = client.list_role_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_role_option(auth: &Auth, api_id: Option<Uuid>, user_id: Option<Uuid>, name: Option<&str>)
    -> Result<Vec<RoleSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleOption {
        api_id: api_id.map(|id| id.as_bytes().to_vec()),
        user_id: user_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned())
    });
    let response = client.list_role_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_role(auth: &Auth, id: Uuid, api_id: Uuid, name: &str, multi: bool, ip_lock: bool, access_duration: i32, refresh_duration: i32)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleSchema {
        id: id.as_bytes().to_vec(),
        api_id: api_id.as_bytes().to_vec(),
        name: name.to_owned(),
        multi,
        ip_lock,
        access_duration,
        refresh_duration,
        access_key: Vec::new(),
        procedure_ids: Vec::new()
    });
    let response = client.create_role(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_role(auth: &Auth, id: Uuid, name: Option<&str>, multi: Option<bool>, ip_lock: Option<bool>, access_duration: Option<i32>, refresh_duration: Option<i32>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        multi,
        ip_lock,
        access_duration,
        refresh_duration
    });
    let _response = client.update_role(request)
        .await?
        .into_inner();
    Ok(())
}

pub(crate) async fn delete_role(auth: &Auth, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleId {
        id: id.as_bytes().to_vec()
    });
    let _response = client.delete_role(request)
        .await?
        .into_inner();
    Ok(())
}

pub(crate) async fn add_role_access(auth: &Auth, id: Uuid, procedure_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleAccess {
        id: id.as_bytes().to_vec(),
        procedure_id: procedure_id.as_bytes().to_vec()
    });
    let _response = client.add_role_access(request)
        .await?
        .into_inner();
    Ok(())
}

pub(crate) async fn remove_role_access(auth: &Auth, id: Uuid, procedure_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        RoleServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleAccess {
        id: id.as_bytes().to_vec(),
        procedure_id: procedure_id.as_bytes().to_vec()
    });
    let _response = client.remove_role_access(request)
        .await?
        .into_inner();
    Ok(())
}
