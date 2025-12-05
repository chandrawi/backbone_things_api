use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_proto::auth::auth::auth_service_client::AuthServiceClient;
use bbthings_grpc_proto::auth::auth::{
    UserKeyRequest, UserLoginRequest, UserLoginResponse,
    UserRefreshRequest, UserRefreshResponse,
    UserLogoutRequest, UserLogoutResponse
};
use crate::auth::Auth;
use bbthings_grpc_server::utility::{import_public_key, encrypt_message};

const KEY_IMPORT_ERR: &str = "key import error";
const ENCRYPT_ERR: &str = "encrypt password error";

pub(crate) async fn user_login(auth: &Auth, username: &str, password: &str)
    -> Result<UserLoginResponse, Status>
{
    let mut client = AuthServiceClient::new(auth.channel.to_owned());
    let request = Request::new(UserKeyRequest {
    });
    // get transport public key of requested user and encrypt the password
    let response = client.user_login_key(request).await?.into_inner();
    let pub_key = import_public_key(response.public_key.as_slice())
        .map_err(|_| Status::internal(KEY_IMPORT_ERR))?;
    let passhash = encrypt_message(password.as_bytes(), pub_key)
        .map_err(|_| Status::internal(ENCRYPT_ERR))?;
    // request access and refresh tokens
    let request = Request::new(UserLoginRequest {
        username: username.to_owned(),
        password: passhash
    });
    let response = client.user_login(request).await?.into_inner();
    Ok(response)
}

pub(crate) async fn user_refresh(auth: &Auth, api_id: Uuid, access_token: &str, refresh_token: &str)
    -> Result<UserRefreshResponse, Status>
{
    let mut client = AuthServiceClient::new(auth.channel.to_owned());
    let request = Request::new(UserRefreshRequest {
        api_id: api_id.as_bytes().to_vec(),
        access_token: access_token.to_owned(),
        refresh_token: refresh_token.to_owned(),
    });
    let response = client.user_refresh(request).await?.into_inner();
    Ok(response)
}

pub(crate) async fn user_logout(auth: &Auth, user_id: Uuid, auth_token: &str)
    -> Result<UserLogoutResponse, Status>
{
    let mut client = AuthServiceClient::new(auth.channel.to_owned());
    let request = Request::new(UserLogoutRequest {
        user_id: user_id.as_bytes().to_vec(),
        auth_token: auth_token.to_owned()
    });
    let response = client.user_logout(request).await?.into_inner();
    Ok(response)
}
