use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_database::{ProfileMode, DataType, DataValue};
use bbthings_grpc_proto::auth::profile::profile_service_client::ProfileServiceClient;
use bbthings_grpc_proto::auth::profile::{
    ProfileId, RoleId, UserId, RoleProfileSchema, RoleProfileUpdate, 
    UserProfileSchema, UserProfileUpdate, UserProfileSwap
};
use crate::auth::Auth;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const PROFILE_NOT_FOUND: &str = "requested profile not found";

pub(crate) async fn read_role_profile(auth: &Auth, id: i32)
    -> Result<RoleProfileSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProfileId {
        id
    });
    let response = client.read_role_profile(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(PROFILE_NOT_FOUND))?)
}

pub(crate) async fn list_role_profile_by_role(auth: &Auth, role_id: Uuid)
    -> Result<Vec<RoleProfileSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleId {
        id: role_id.as_bytes().to_vec()
    });
    let response = client.list_role_profile(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_role_profile(auth: &Auth, role_id: Uuid, name: &str, value_type: DataType, mode: ProfileMode)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleProfileSchema {
        id: 0,
        role_id: role_id.as_bytes().to_vec(),
        name: name.to_owned(),
        value_type: value_type.into(),
        mode: mode.into()
    });
    let response = client.create_role_profile(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn update_role_profile(auth: &Auth, id: i32, name: Option<&str>, value_type: Option<DataType>, mode: Option<ProfileMode>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(RoleProfileUpdate {
        id,
        name: name.map(|s| s.to_owned()),
        value_type: value_type.map(|v| v.into()),
        mode: mode.map(|v| v.into())
    });
    client.update_role_profile(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_role_profile(auth: &Auth, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProfileId {
        id
    });
    client.delete_role_profile(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_user_profile(auth: &Auth, id: i32)
    -> Result<UserProfileSchema, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProfileId {
        id
    });
    let response = client.read_user_profile(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(PROFILE_NOT_FOUND))?)
}

pub(crate) async fn list_user_profile_by_user(auth: &Auth, user_id: Uuid)
    -> Result<Vec<UserProfileSchema>, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserId {
        id: user_id.as_bytes().to_vec()
    });
    let response = client.list_user_profile(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_user_profile(auth: &Auth, user_id: Uuid, name: &str, value: DataValue)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserProfileSchema {
        id: 0,
        user_id: user_id.as_bytes().to_vec(),
        name: name.to_owned(),
        value_bytes: value.to_bytes(),
        value_type: value.get_type().into(),
        order: 0
    });
    let response = client.create_user_profile(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn update_user_profile(auth: &Auth, id: i32, name: Option<&str>, value: Option<DataValue>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserProfileUpdate {
        id,
        name: name.map(|s| s.to_owned()),
        value_type: value.clone().map(|v| v.get_type().into()),
        value_bytes: value.map(|v| v.to_bytes())
    });
    client.update_user_profile(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_user_profile(auth: &Auth, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(ProfileId {
        id
    });
    client.delete_user_profile(request)
        .await?;
    Ok(())
}

pub(crate) async fn swap_user_profile(auth: &Auth, user_id: Uuid, name: &str, order_1: i16, order_2: i16)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(auth.auth_token.clone());
    let mut client = 
        ProfileServiceClient::with_interceptor(auth.channel.to_owned(), interceptor);
    let request = Request::new(UserProfileSwap {
        user_id: user_id.as_bytes().to_vec(),
        name: name.to_owned(),
        order_1: order_1 as u32,
        order_2: order_2 as u32
    });
    client.swap_user_profile(request)
        .await?;
    Ok(())
}
