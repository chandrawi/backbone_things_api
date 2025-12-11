use tonic::{Request, Response, Status, transport::Channel};
use uuid::Uuid;
use chrono::{Duration, Utc};
use bbthings_database::Auth;
use bbthings_database::utility::generate_access_key;
use crate::proto::auth::auth::auth_service_server::AuthService;
use crate::proto::auth::auth::{
    ApiKeyRequest, ApiKeyResponse, ApiLoginRequest, ApiLoginResponse,
    UserKeyRequest, UserKeyResponse, UserLoginRequest, UserLoginResponse,
    UserRefreshRequest, UserRefreshResponse, UserLogoutRequest, UserLogoutResponse,
    ProcedureMap, AccessTokenMap
};
use crate::proto::auth::auth::auth_service_client::AuthServiceClient;
use crate::common::{token, utility, utility::handle_error};
use crate::common::config::{ROOT_NAME, ROOT_DATA, API_KEY, USER_KEY, TransportKey};

const TOKEN_NOT_FOUND: &str = "requested token not found";
const PASSWORD_MISMATCH: &str = "password does not match";
const GENERATE_TOKEN_ERR: &str = "error generate token";
const TOKEN_MISMATCH: &str = "token is not match";
const TOKEN_UNVERIFIED: &str = "token unverified";
const REFRESH_EXPIRED: &str = "refresh token is expired";

pub struct AuthServer {
    pub auth_db: Auth
}

impl AuthServer {
    pub fn new(auth_db: Auth) -> Self {
        AuthServer {
            auth_db
        }
    }
}

#[tonic::async_trait]
impl AuthService for AuthServer {

    async fn api_password_key(&self, _: Request<ApiKeyRequest>)
        -> Result<Response<ApiKeyResponse>, Status>
    {
        let api_key = API_KEY.get_or_init(|| TransportKey::new());
        let public_key = api_key.public_der.clone();
        Ok(Response::new(ApiKeyResponse { public_key }))
    }

    async fn api_login(&self, request: Request<ApiLoginRequest>)
        -> Result<Response<ApiLoginResponse>, Status>
    {
        let request = request.into_inner();
        let id = Uuid::from_slice(&request.api_id).unwrap_or_default();
        let result = self.auth_db.read_api(id).await;
        let (access_key, access_procedures) = match result {
            Ok(api) => {
                // decrypt encrypted password hash and return error if password is not verified
                let api_key = API_KEY.get_or_init(|| TransportKey::new());
                let priv_key = api_key.private_key.clone();
                let password = utility::decrypt_message(&request.password, priv_key)?;
                let hash = api.password.clone();
                utility::verify_password(&password, &hash)
                    .map_err(|_| Status::invalid_argument(PASSWORD_MISMATCH))?;
                // update api with generated access key
                let key = generate_access_key();
                self.auth_db.update_api(id, None, None, None, None, None, Some(&key)).await
                    .map_err(|e| handle_error(e))?;
                let access_key = utility::encrypt_message(&key, &request.public_key)?;
                let procedures = api.procedures.into_iter()
                    .map(|e| ProcedureMap { procedure: e.name, roles: e.roles })
                    .collect();
                (access_key, procedures)
            },
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(ApiLoginResponse { access_key, access_procedures }))
    }

    async fn user_password_key(&self, _: Request<UserKeyRequest>)
        -> Result<Response<UserKeyResponse>, Status>
    {
        let user_key = USER_KEY.get_or_init(|| TransportKey::new());
        let public_key = user_key.public_der.clone();
        Ok(Response::new(UserKeyResponse { public_key }))
    }

    async fn user_login(&self, request: Request<UserLoginRequest>)
        -> Result<Response<UserLoginResponse>, Status>
    {
        let mut remote_ip = request.remote_addr().map(|s| match s.ip() {
                std::net::IpAddr::V4(v) => v.octets().to_vec(),
                std::net::IpAddr::V6(v) => v.octets().to_vec()
            }).unwrap_or(Vec::new());
        let request = request.into_inner();
        // Get user schema from root data or database
        let result = if &request.username == ROOT_NAME {
            let root = ROOT_DATA.get().map(|x| x.to_owned()).unwrap_or_default();
            Ok(root.into())
        } else {
            self.auth_db.read_user_by_name(&request.username).await
                .map_err(|e| handle_error(e))
        };
        let (user_id, auth_token, access_tokens) = match result {
            Ok(user) => {
                // decrypt encrypted password hash and return error if password is not verified
                let user_key = USER_KEY.get_or_init(|| TransportKey::new());
                let priv_key = user_key.private_key.clone();
                let password = utility::decrypt_message(&request.password, priv_key)?;
                let hash = user.password.clone();
                if user.name == ROOT_NAME {
                    // add delay to overcome brute force attack
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    if user.password.into_bytes() != password {
                        return Err(Status::invalid_argument(PASSWORD_MISMATCH))
                    }
                } else {
                    utility::verify_password(&password, &hash)
                        .map_err(|_| Status::invalid_argument(PASSWORD_MISMATCH))?;
                }
                // delete all previous token if one of the roles marked as non multi device login
                let multi = user.roles.iter().map(|e| e.multi).filter(|&e| !e).count();
                if multi > 0 {
                    self.auth_db.delete_token_by_user(user.id).await
                    .map_err(|e| handle_error(e))?;
                }
                let ip_lock = user.roles.iter().map(|e| e.ip_lock).filter(|&e| e).count();
                if ip_lock == 0 {
                    remote_ip = Vec::new();
                }
                // get minimum refresh duration of roles associated with the user and calculate refresh expire
                let duration = user.roles.iter().map(|e| e.refresh_duration).min().unwrap_or_default();
                let expire = Utc::now() + Duration::seconds(duration as i64);
                // insert new tokens as a number of user role and get generated access id, refresh token, and auth token
                let token_sets = self.auth_db
                    .create_auth_token(user.id, expire, &remote_ip, user.roles.len())
                    .await
                    .map_err(|e| handle_error(e))?;
                // get auth_token from token set. note that auth_token is the same on every set
                let auth_token = match token_sets.get(0) {
                    Some(value) => value.2.clone(),
                    None => String::new()
                };
                // generate access tokens using data from user role and generated access id
                let mut iter_tokens = token_sets.into_iter();
                let tokens: Vec<AccessTokenMap> = user.roles.iter()
                    .map(|e| {
                        let generate = iter_tokens.next().unwrap_or_default();
                        AccessTokenMap {
                            api_id: e.api_id.as_bytes().to_vec(),
                            access_token: token::generate_token(generate.0, &e.role, e.access_duration, &e.access_key)
                                .unwrap_or(String::new()),
                            refresh_token: generate.1
                        }
                    })
                    .filter(|e| e.access_token != String::new())
                    .collect();
                if user.roles.len() != tokens.len() {
                    return Err(Status::internal(GENERATE_TOKEN_ERR));
                }
                (user.id, auth_token, tokens)
            },
            Err(e) => return Err(e)
        };
        Ok(Response::new(UserLoginResponse { user_id: user_id.as_bytes().to_vec(), auth_token, access_tokens }))
    }

    async fn user_refresh(&self, request: Request<UserRefreshRequest>)
        -> Result<Response<UserRefreshResponse>, Status>
    {
        let remote_ip = request.remote_addr().map(|s| match s.ip() {
                std::net::IpAddr::V4(v) => v.octets().to_vec(),
                std::net::IpAddr::V6(v) => v.octets().to_vec()
            }).unwrap_or(Vec::new());
        let request = request.into_inner();
        let result = self.auth_db.read_api(Uuid::from_slice(&request.api_id).unwrap_or_default()).await;
        let (access_key, token_claims) = match result {
            Ok(api) => {
                // verify access token and get token claims
                let mut decoded = token::decode_token(&request.access_token, &api.access_key, false);
                if decoded.is_none() {
                    let root = ROOT_DATA.get().map(|x| x.to_owned()).unwrap_or_default();
                    decoded = token::decode_token(&request.access_token, &root.access_key, false);
                }
                let token_claims = match decoded {
                    Some(value) => value,
                    None => return Err(Status::unauthenticated(TOKEN_UNVERIFIED))
                };
                (api.access_key, token_claims)
            },
            Err(e) => {
                return Err(handle_error(e));
            }
        };
        let result = self.auth_db.read_access_token(token_claims.jti).await;
        let (refresh_token, access_token) = match result {
            Ok(token) => {
                // check if current time exceed expired time (created token time plus refresh duration)
                if token.expired < Utc::now() {
                    return Err(Status::unauthenticated(REFRESH_EXPIRED));
                }
                // check if remote ip match with stored login ip
                let ip_match = if token.ip == Vec::<u8>::new() {
                    true
                } else {
                    token.ip == remote_ip
                };
                // update token in database and generate new access token if refresh token match
                if token.refresh_token == request.refresh_token && ip_match {
                    let refresh_token = self.auth_db
                        .update_access_token(token_claims.jti, Some(token.expired), None).await
                        .map_err(|e| handle_error(e))?;
                    let duration = (token_claims.exp - token_claims.iat) as i32;
                    let access_token = token::generate_token(token_claims.jti, &token_claims.sub, duration, &access_key)
                        .ok_or_else(|| Status::internal(GENERATE_TOKEN_ERR))?;
                    (refresh_token, access_token)
                } else {
                    return Err(Status::invalid_argument(TOKEN_MISMATCH))
                }
            },
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(UserRefreshResponse { refresh_token, access_token }))
    }

    async fn user_logout(&self, request: Request<UserLogoutRequest>)
        -> Result<Response<UserLogoutResponse>, Status>
    {
        let request = request.into_inner();
        // delete all tokens in database associated with input auth token and user id
        let result = self.auth_db.list_auth_token(&request.auth_token).await;
        let tokens = match result {
            Ok(tokens) => tokens,
            Err(e) => return Err(handle_error(e))
        };
        match tokens.into_iter().next() {
            Some(token) => {
                if token.user_id.as_bytes().to_vec() == request.user_id {
                    self.auth_db.delete_auth_token(&request.auth_token).await
                        .map_err(|e| handle_error(e))?;
                } else {
                    return Err(Status::invalid_argument(TOKEN_MISMATCH));
                }
            },
            None => return Err(Status::not_found(TOKEN_NOT_FOUND))
        }
        Ok(Response::new(UserLogoutResponse { }))
    }

}

pub async fn api_login(addr: &str, api_id: Uuid, password: &str)
    -> Option<ApiLoginResponse>
{
    let channel = Channel::from_shared(addr.to_owned())
        .expect("Invalid address")
        .connect()
        .await
        .expect(&format!("Error making channel to {}", addr));
    let mut client = AuthServiceClient::new(channel.to_owned());
    let request = Request::new(ApiKeyRequest {
    });
    // get transport public key of requested API and encrypt the password
    let response = client.api_password_key(request).await.ok()?.into_inner();
    let passhash = utility::encrypt_message(password.as_bytes(), response.public_key.as_slice()).ok()?;
    // request API key and procedures access from server
    let (priv_key, pub_key) = utility::generate_transport_keys().ok()?;
    let pub_der = utility::export_public_key(pub_key).ok()?;
    let request = Request::new(ApiLoginRequest {
        api_id: api_id.as_bytes().to_vec(),
        password: passhash,
        public_key: pub_der
    });
    let mut response = client.api_login(request).await.ok()?.into_inner();
    response.access_key = utility::decrypt_message(&response.access_key, priv_key).ok()?;
    Some(response)
}
