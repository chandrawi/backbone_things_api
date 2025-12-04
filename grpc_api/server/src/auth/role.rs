use tonic::{Request, Response, Status};
use uuid::Uuid;
use bbthings_database::Auth;
use bbthings_grpc_proto::auth::role::role_service_server::RoleService;
use bbthings_grpc_proto::auth::role::{
    RoleSchema, RoleId, RoleIds, RoleName, ApiId, UserId, RoleOption, RoleUpdate, RoleAccess,
    RoleReadResponse, RoleListResponse, RoleCreateResponse, RoleChangeResponse
};
use crate::common::validator::{AuthValidator, ValidatorKind};
use crate::common::utility::handle_error;

pub struct RoleServer {
    auth_db: Auth,
    validator_flag: bool
}

impl RoleServer {
    pub fn new(auth_db: Auth) -> Self {
        RoleServer {
            auth_db,
            validator_flag: false
        }
    }
    pub fn new_with_validator(auth_db: Auth) -> Self {
        RoleServer {
            auth_db,
            validator_flag: true
        }
    }
}

#[tonic::async_trait]
impl RoleService for RoleServer {

    async fn read_role(&self, request: Request<RoleId>)
        -> Result<Response<RoleReadResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.read_role(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleReadResponse { result }))
    }

    async fn read_role_by_name(&self, request: Request<RoleName>)
        -> Result<Response<RoleReadResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.read_role_by_name(
            Uuid::from_slice(&request.api_id).unwrap_or_default(),
            &request.name
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleReadResponse { result }))
    }

    async fn list_role_by_ids(&self, request: Request<RoleIds>)
        -> Result<Response<RoleListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_role_by_ids(
            request.ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>().as_slice()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleListResponse { results }))
    }

    async fn list_role_by_api(&self, request: Request<ApiId>)
        -> Result<Response<RoleListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_role_by_api(Uuid::from_slice(&request.api_id).unwrap_or_default()).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleListResponse { results }))
    }

    async fn list_role_by_user(&self, request: Request<UserId>)
        -> Result<Response<RoleListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_role_by_user(Uuid::from_slice(&request.user_id).unwrap_or_default()).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleListResponse { results }))
    }

    async fn list_role_by_name(&self, request: Request<RoleName>)
        -> Result<Response<RoleListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_role_by_name(&request.name).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleListResponse { results }))
    }

    async fn list_role_option(&self, request: Request<RoleOption>)
        -> Result<Response<RoleListResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.list_role_option(
            request.api_id.map(|id| Uuid::from_slice(&id).unwrap_or_default()),
            request.user_id.map(|id| Uuid::from_slice(&id).unwrap_or_default()),
            request.name.as_deref()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleListResponse { results }))
    }

    async fn create_role(&self, request: Request<RoleSchema>)
        -> Result<Response<RoleCreateResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.create_role(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.api_id).unwrap_or_default(),
            &request.name,
            request.multi,
            request.ip_lock,
            request.access_duration,
            request.refresh_duration
        ).await;
        let id = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleCreateResponse { id: id.as_bytes().to_vec() }))
    }

    async fn update_role(&self, request: Request<RoleUpdate>)
        -> Result<Response<RoleChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.update_role(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            request.name.as_deref(),
            request.multi,
            request.ip_lock,
            request.access_duration,
            request.refresh_duration
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleChangeResponse { }))
    }

    async fn delete_role(&self, request: Request<RoleId>)
        -> Result<Response<RoleChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.delete_role(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleChangeResponse { }))
    }

    async fn add_role_access(&self, request: Request<RoleAccess>)
        -> Result<Response<RoleChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.add_role_access(
            Uuid::from_slice(&request.id).unwrap_or_default(), 
            Uuid::from_slice(&request.procedure_id).unwrap_or_default()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleChangeResponse { }))
    }

    async fn remove_role_access(&self, request: Request<RoleAccess>)
        -> Result<Response<RoleChangeResponse>, Status>
    {
        self.validate(request.extensions(), ValidatorKind::Root).await?;
        let request = request.into_inner();
        let result = self.auth_db.remove_role_access(
            Uuid::from_slice(&request.id).unwrap_or_default(), 
            Uuid::from_slice(&request.procedure_id).unwrap_or_default()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(RoleChangeResponse { }))
    }

}

impl AuthValidator for RoleServer {

    fn validator_flag(&self) -> bool {
        self.validator_flag
    }

    fn auth_db(&self) ->  &Auth {
        &self.auth_db
    }

}
