use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_proto::auth::user::user_service_client::UserServiceClient;
use bbthings_grpc_proto::auth::user::{
    UserId, UserIds, UserName, ApiId, RoleId, UserOption, UserSchema, UserUpdate, UserRole
};
use crate::auth::Auth;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const USER_NOT_FOUND: &str = "requested user not found";

pub(crate) async fn read_user(auth: &Auth, id: Uuid)
    -> Result<UserSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_user(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(USER_NOT_FOUND))?)
}

pub(crate) async fn read_user_by_name(auth: &Auth, name: &str)
    -> Result<UserSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserName {
        name: name.to_owned()
    });
    let response = client.read_user_by_name(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(USER_NOT_FOUND))?)
}

pub(crate) async fn list_user_by_ids(auth: &Auth, ids: &[Uuid])
    -> Result<Vec<UserSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_user_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_user_by_api(auth: &Auth, api_id: Uuid)
    -> Result<Vec<UserSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ApiId {
        id: api_id.as_bytes().to_vec()
    });
    let response = client.list_user_by_api(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_user_by_role(auth: &Auth, role_id: Uuid)
    -> Result<Vec<UserSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleId {
        id: role_id.as_bytes().to_vec()
    });
    let response = client.list_user_by_role(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_user_by_name(auth: &Auth, name: &str)
    -> Result<Vec<UserSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserName {
        name: name.to_owned()
    });
    let response = client.list_user_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_user_option(auth: &Auth, api_id: Option<Uuid>, role_id: Option<Uuid>, name: Option<&str>)
    -> Result<Vec<UserSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserOption {
        api_id: api_id.map(|id| id.as_bytes().to_vec()),
        role_id: role_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned())
    });
    let response = client.list_user_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_user(auth: &Auth, id: Uuid, name: &str, email: &str, phone: &str, password: &str)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserSchema {
        id: id.as_bytes().to_vec(),
        name: name.to_owned(),
        email: email.to_owned(),
        phone: phone.to_owned(),
        password: password.to_owned(),
        roles: Vec::new()
    });
    let response = client.create_user(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_user(auth: &Auth, id: Uuid, name: Option<&str>, email: Option<&str>, phone: Option<&str>, password: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserUpdate {
        id: id.as_bytes().to_vec(),
        name: name.map(|s| s.to_owned()),
        email: email.map(|s| s.to_owned()),
        phone: phone.map(|s| s.to_owned()),
        password: password.map(|s| s.to_owned())
    });
    client.update_user(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_user(auth: &Auth, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserId {
        id: id.as_bytes().to_vec()
    });
    client.delete_user(request)
        .await?;
    Ok(())
}

pub(crate) async fn add_user_role(auth: &Auth, id: Uuid, role_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserRole {
        user_id: id.as_bytes().to_vec(),
        role_id: role_id.as_bytes().to_vec()
    });
    client.add_user_role(request)
        .await?;
    Ok(())
}

pub(crate) async fn remove_user_role(auth: &Auth, id: Uuid, role_id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        UserServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserRole {
        user_id: id.as_bytes().to_vec(),
        role_id: role_id.as_bytes().to_vec()
    });
    client.remove_user_role(request)
        .await?;
    Ok(())
}
