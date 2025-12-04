use tonic::{Request, Response, Status};
use chrono::{Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::Auth;
use bbthings_grpc_proto::auth::token::token_service_server::TokenService;
use bbthings_grpc_proto::auth::token::{
    TokenSchema, AuthToken, AccessId, UserId, AuthTokenCreate, TokenUpdate,
    TokenTime, TokenRangeSingle, TokenRangeDouble,
    TokenReadResponse, TokenListResponse, TokenCreateResponse, AuthTokenCreateResponse, 
    TokenUpdateResponse, TokenChangeResponse
};
use crate::common::validator::{AuthValidator, ValidatorKind};
use crate::common::utility::handle_error;

pub struct TokenServer {
    auth_db: Auth,
    validator_flag: bool
}

impl TokenServer {
    pub fn new(auth_db: Auth) -> Self {
        TokenServer {
            auth_db,
            validator_flag: false
        }
    }
    pub fn new_with_validator(auth_db: Auth) -> Self {
        TokenServer {
            auth_db,
            validator_flag: true
        }
    }
}

#[tonic::async_trait]
impl TokenService for TokenServer {

    async fn read_access_token(&self, request: Request<AccessId>)
        -> Result<Response<TokenReadResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.read_access_token(request.access_id).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenReadResponse { result }))
    }

    async fn list_auth_token(&self, request: Request<AuthToken>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_auth_token(&request.auth_token).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_user(&self, request: Request<UserId>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_user(Uuid::from_slice(&request.user_id).unwrap_or_default()).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_created_earlier(&self, request: Request<TokenTime>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_created_earlier(
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_created_later(&self, request: Request<TokenTime>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_created_later(
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_created_range(&self, request: Request<TokenRangeSingle>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_created_range(
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_expired_earlier(&self, request: Request<TokenTime>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_expired_earlier(
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_expired_later(&self, request: Request<TokenTime>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_expired_later(
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_expired_range(&self, request: Request<TokenRangeSingle>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_expired_range(
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn list_token_by_range(&self, request: Request<TokenRangeDouble>)
        -> Result<Response<TokenListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_token_by_range(
            Utc.timestamp_nanos(request.begin_1 * 1000),
            Utc.timestamp_nanos(request.end_1 * 1000),
            Utc.timestamp_nanos(request.begin_2 * 1000),
            Utc.timestamp_nanos(request.end_2 * 1000),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default())
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenListResponse { results }))
    }

    async fn create_access_token(&self, request: Request<TokenSchema>)
        -> Result<Response<TokenCreateResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.create_access_token(
            Uuid::from_slice(&request.user_id).unwrap_or_default(),
            &request.auth_token,
            Utc.timestamp_nanos(request.expired * 1000),
            request.ip.as_slice()
        ).await;
        let (access_id, refresh_token, auth_token) = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenCreateResponse { access_id, refresh_token, auth_token }))
    }

    async fn create_auth_token(&self, request: Request<AuthTokenCreate>)
        -> Result<Response<AuthTokenCreateResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.create_auth_token(
            Uuid::from_slice(&request.user_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.expire * 1000),
            request.ip.as_slice(),
            request.number as usize
        ).await;
        let tokens = match result {
            Ok(value) => value.into_iter()
                .map(|t| TokenCreateResponse {
                    access_id: t.0,
                    refresh_token: t.1,
                    auth_token: t.2
                }).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(AuthTokenCreateResponse { tokens }))
    }

    async fn update_access_token(&self, request: Request<TokenUpdate>)
        -> Result<Response<TokenUpdateResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.update_access_token(
            request.access_id.unwrap_or_default(),
            request.expire.map(|s| Utc.timestamp_nanos(s * 1000)),
            request.ip.as_deref()
        ).await;
        let refresh_token = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenUpdateResponse { refresh_token }))
    }

    async fn update_auth_token( &self, request: Request<TokenUpdate>)
        -> Result<Response<TokenUpdateResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.update_auth_token(
            request.auth_token.unwrap_or_default().as_ref(),
            request.expire.map(|s| Utc.timestamp_nanos(s * 1000)),
            request.ip.as_deref()
        ).await;
        let refresh_token = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenUpdateResponse { refresh_token }))
    }

    async fn delete_access_token(&self, request: Request<AccessId>)
        -> Result<Response<TokenChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.delete_access_token(request.access_id).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenChangeResponse { }))
    }

    async fn delete_auth_token(&self, request: Request<AuthToken>)
        -> Result<Response<TokenChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.delete_auth_token(&request.auth_token).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenChangeResponse { }))
    }

    async fn delete_token_by_user(&self, request: Request<UserId>)
        -> Result<Response<TokenChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.delete_token_by_user(Uuid::from_slice(&request.user_id).unwrap_or_default()).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TokenChangeResponse { }))
    }

}

impl AuthValidator for TokenServer {

    fn validator_flag(&self) -> bool {
        self.validator_flag
    }

    fn auth_db(&self) ->  &Auth {
        &self.auth_db
    }

}
