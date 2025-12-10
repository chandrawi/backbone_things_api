use tonic::{Request, Status, transport::Channel};
use uuid::Uuid;
use bbthings_grpc_server::proto::auth::auth::auth_service_client::AuthServiceClient;
use bbthings_grpc_server::proto::auth::auth::{
    UserKeyRequest, UserLoginRequest, UserLoginResponse,
    UserRefreshRequest, UserRefreshResponse,
    UserLogoutRequest, UserLogoutResponse
};
use bbthings_grpc_server::utility::encrypt_message;

pub(crate) async fn user_login(channel: &Channel, username: &str, password: &str)
    -> Result<UserLoginResponse, Status>
{
    let mut client = AuthServiceClient::new(channel.to_owned());
    let request = Request::new(UserKeyRequest {
    });
    // get transport public key of requested user and encrypt the password
    let response = client.user_password_key(request).await?.into_inner();
    let passhash = encrypt_message(password.as_bytes(), &response.public_key)?;
    // request access and refresh tokens
    let request = Request::new(UserLoginRequest {
        username: username.to_owned(),
        password: passhash
    });
    let response = client.user_login(request).await?.into_inner();
    Ok(response)
}

pub(crate) async fn user_refresh(channel: &Channel, api_id: Uuid, access_token: &str, refresh_token: &str)
    -> Result<UserRefreshResponse, Status>
{
    let mut client = AuthServiceClient::new(channel.to_owned());
    let request = Request::new(UserRefreshRequest {
        api_id: api_id.as_bytes().to_vec(),
        access_token: access_token.to_owned(),
        refresh_token: refresh_token.to_owned(),
    });
    let response = client.user_refresh(request).await?.into_inner();
    Ok(response)
}

pub(crate) async fn user_logout(channel: &Channel, user_id: Uuid, auth_token: &str)
    -> Result<UserLogoutResponse, Status>
{
    let mut client = AuthServiceClient::new(channel.to_owned());
    let request = Request::new(UserLogoutRequest {
        user_id: user_id.as_bytes().to_vec(),
        auth_token: auth_token.to_owned()
    });
    let response = client.user_logout(request).await?.into_inner();
    Ok(response)
}
