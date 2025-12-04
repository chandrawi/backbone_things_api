use tonic::{Request, Response, Status};
use uuid::Uuid;
use bbthings_database::Resource;
use bbthings_grpc_proto::resource::set::set_service_server::SetService;
use bbthings_grpc_proto::resource::set::{
    SetSchema, SetId, SetIds, SetName, SetOption, SetUpdate, SetMemberRequest, SetMemberSwap,
    SetTemplateSchema, SetTemplateId, SetTemplateIds, SetTemplateName, SetTemplateOption, 
    SetTemplateUpdate, SetTemplateMemberRequest, SetTemplateMemberSwap,
    SetReadResponse, SetListResponse, SetCreateResponse, SetChangeResponse, 
    TemplateReadResponse, TemplateListResponse, TemplateCreateResponse, TemplateChangeResponse
};
use crate::common::validator::{AccessValidator, AccessSchema};
use crate::common::utility::handle_error;

const READ_SET: &str = "read_set";
const CREATE_SET: &str = "create_set";
const UPDATE_SET: &str = "update_set";
const DELETE_SET: &str = "delete_set";
const CHANGE_SET_MEMBER: &str = "change_set_member";

pub struct SetServer {
    resource_db: Resource,
    token_key: Vec<u8>,
    accesses: Vec<AccessSchema>
}

impl SetServer {
    pub fn new(resource_db: Resource) -> Self {
        Self {
            resource_db,
            token_key: Vec::new(),
            accesses: Vec::new()
        }
    }
    pub fn new_with_validator(resource_db: Resource, token_key: &[u8], accesses: &[AccessSchema]) -> Self {
        const PROCEDURES: &[&str] = &[
            READ_SET, CREATE_SET, UPDATE_SET, DELETE_SET, CHANGE_SET_MEMBER
        ];
        Self {
            resource_db,
            token_key: token_key.to_vec(),
            accesses: Self::construct_accesses(accesses, PROCEDURES)
        }
    }
}

#[tonic::async_trait]
impl SetService for SetServer {

    async fn read_set(&self, request: Request<SetId>)
        -> Result<Response<SetReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.read_set(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetReadResponse { result }))
    }

    async fn list_set_by_ids(&self, request: Request<SetIds>)
        -> Result<Response<SetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_by_ids(
            request.ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>().as_slice()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetListResponse { results }))
    }

    async fn list_set_by_template(&self, request: Request<SetTemplateId>)
        -> Result<Response<SetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_by_template(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetListResponse { results }))
    }

    async fn list_set_by_name(&self, request: Request<SetName>)
        -> Result<Response<SetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_by_name(&request.name).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetListResponse { results }))
    }

    async fn list_set_option(&self, request: Request<SetOption>)
        -> Result<Response<SetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_option(
            request.template_id.map(|id| Uuid::from_slice(&id).unwrap_or_default()),
            request.name.as_deref()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetListResponse { results }))
    }

    async fn create_set(&self, request: Request<SetSchema>)
        -> Result<Response<SetCreateResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.create_set(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.template_id).unwrap_or_default(),
            &request.name,
            Some(&request.description)
        ).await;
        let id = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetCreateResponse { id: id.as_bytes().to_vec() }))
    }

    async fn update_set(&self, request: Request<SetUpdate>)
        -> Result<Response<SetChangeResponse>, Status>
    {
        self.validate(request.extensions(), UPDATE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.update_set(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            request.template_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.name.as_deref(),
            request.description.as_deref()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetChangeResponse { }))
    }

    async fn delete_set(&self, request: Request<SetId>)
        -> Result<Response<SetChangeResponse>, Status>
    {
        self.validate(request.extensions(), DELETE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.delete_set(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetChangeResponse { }))
    }

    async fn add_set_member(&self, request: Request<SetMemberRequest>)
        -> Result<Response<SetChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.add_set_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            &request.data_index
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetChangeResponse { }))
    }

    async fn remove_set_member(&self, request: Request<SetMemberRequest>)
        -> Result<Response<SetChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.remove_set_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetChangeResponse { }))
    }

    async fn swap_set_member(&self, request: Request<SetMemberSwap>)
        -> Result<Response<SetChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.swap_set_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.device_id_1).unwrap_or_default(),
            Uuid::from_slice(&request.model_id_1).unwrap_or_default(),
            Uuid::from_slice(&request.device_id_2).unwrap_or_default(),
            Uuid::from_slice(&request.model_id_2).unwrap_or_default()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(SetChangeResponse { }))
    }

    async fn read_set_template(&self, request: Request<SetTemplateId>)
        -> Result<Response<TemplateReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.read_set_template(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateReadResponse { result }))
    }

    async fn list_set_template_by_ids(&self, request: Request<SetTemplateIds>)
        -> Result<Response<TemplateListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_template_by_ids(
            request.ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>().as_slice()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateListResponse { results }))
    }

    async fn list_set_template_by_name(&self, request: Request<SetTemplateName>)
        -> Result<Response<TemplateListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_template_by_name(&request.name).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateListResponse { results }))
    }

    async fn list_set_template_option(&self, request: Request<SetTemplateOption>)
        -> Result<Response<TemplateListResponse>, Status>
    {
        self.validate(request.extensions(), READ_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.list_set_template_option(
            request.name.as_deref()
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateListResponse { results }))
    }

    async fn create_set_template(&self, request: Request<SetTemplateSchema>)
        -> Result<Response<TemplateCreateResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.create_set_template(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            &request.name,
            Some(&request.description)
        ).await;
        let id = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateCreateResponse { id: id.as_bytes().to_vec() }))
    }

    async fn update_set_template(&self, request: Request<SetTemplateUpdate>)
        -> Result<Response<TemplateChangeResponse>, Status>
    {
        self.validate(request.extensions(), UPDATE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.update_set_template(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            request.name.as_deref(),
            request.description.as_deref()
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateChangeResponse { }))
    }

    async fn delete_set_template(&self, request: Request<SetTemplateId>)
        -> Result<Response<TemplateChangeResponse>, Status>
    {
        self.validate(request.extensions(), DELETE_SET)?;
        let request = request.into_inner();
        let result = self.resource_db.delete_set_template(Uuid::from_slice(&request.id).unwrap_or_default()).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateChangeResponse { }))
    }

    async fn add_set_template_member(&self, request: Request<SetTemplateMemberRequest>)
        -> Result<Response<TemplateChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.add_set_template_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            Uuid::from_slice(&request.type_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            &request.data_index
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateChangeResponse { }))
    }

    async fn remove_set_template_member(&self, request: Request<SetTemplateMemberRequest>)
        -> Result<Response<TemplateChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.remove_set_template_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            request.template_index as usize
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateChangeResponse { }))
    }

    async fn swap_set_template_member(&self, request: Request<SetTemplateMemberSwap>)
        -> Result<Response<TemplateChangeResponse>, Status>
    {
        self.validate(request.extensions(), CHANGE_SET_MEMBER)?;
        let request = request.into_inner();
        let result = self.resource_db.swap_set_template_member(
            Uuid::from_slice(&request.id).unwrap_or_default(),
            request.template_index_1 as usize,
            request.template_index_2 as usize
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TemplateChangeResponse { }))
    }

}

impl AccessValidator for SetServer {

    fn token_key(&self) -> Vec<u8> {
        self.token_key.clone()
    }

    fn accesses(&self) -> Vec<AccessSchema> {
        self.accesses.clone()
    }

}
