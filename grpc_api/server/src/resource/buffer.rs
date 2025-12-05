use tonic::{Request, Response, Status};
use chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::{Resource, DataType, DataValue, ArrayDataValue};
use bbthings_grpc_proto::resource::buffer::buffer_service_server::BufferService;
use bbthings_grpc_proto::resource::buffer::{
    BufferSchema, BufferMultipleSchema, BufferId, BufferIds, BufferTime, BufferEarlier, BufferLater, BufferRange, BufferNumber, 
    BufferSelector, BuffersSelector, BufferUpdate, BufferUpdateTime,
    BufferGroupTime, BufferGroupEarlier, BufferGroupLater, BufferGroupRange, BufferGroupNumber, BufferGroupSelector, BuffersGroupSelector,
    BufferSetTime, BufferSetEarlier, BufferSetLater, BufferSetRange,
    BufferReadResponse, BufferListResponse, BufferCreateResponse, BufferCreateMultipleResponse, BufferChangeResponse,
    BufferSetReadResponse, BufferSetListResponse, TimestampReadResponse, TimestampListResponse, BufferCountResponse
};
use crate::common::validator::{AccessValidator, AccessSchema};
use crate::common::utility::handle_error;

const READ_BUFFER: &str = "read_buffer";
const CREATE_BUFFER: &str = "create_buffer";
const UPDATE_BUFFER: &str = "update_buffer";
const DELETE_BUFFER: &str = "delete_buffer";

#[derive(Debug)]
pub struct BufferServer {
    resource_db: Resource,
    token_key: Vec<u8>,
    accesses: Vec<AccessSchema>
}

impl BufferServer {
    pub fn new(resource_db: Resource) -> Self {
        Self {
            resource_db,
            token_key: Vec::new(),
            accesses: Vec::new()
        }
    }
    pub fn new_with_validator(resource_db: Resource, token_key: &[u8], accesses: &[AccessSchema]) -> Self {
        const PROCEDURES: &[&str] = &[
            READ_BUFFER, CREATE_BUFFER, UPDATE_BUFFER, DELETE_BUFFER
        ];
        Self {
            resource_db,
            token_key: token_key.to_vec(),
            accesses: Self::construct_accesses(accesses, PROCEDURES)
        }
    }
}

#[tonic::async_trait]
impl BufferService for BufferServer {

    async fn read_buffer(&self, request: Request<BufferId>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer(request.id).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn read_buffer_by_time(&self, request: Request<BufferTime>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_by_time(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn list_buffer_by_ids(&self, request: Request<BufferIds>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_ids(&request.ids).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_time(&self, request: Request<BufferTime>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_time(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_earlier(&self, request: Request<BufferEarlier>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_earlier(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_later(&self, request: Request<BufferLater>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_later(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_range(&self, request: Request<BufferRange>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_range(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_number_before(&self, request: Request<BufferNumber>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_number_before(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.number as usize,
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_by_number_after(&self, request: Request<BufferNumber>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_by_number_after(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.number as usize,
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn read_buffer_first(&self, request: Request<BufferSelector>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_first(
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn read_buffer_last(&self, request: Request<BufferSelector>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_last(
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn list_buffer_first(&self, request: Request<BuffersSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_first(
            request.number as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_first_offset(&self, request: Request<BuffersSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_first_offset(
            request.number as usize,
            request.offset as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_last(&self, request: Request<BuffersSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_last(
            request.number as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_last_offset(&self, request: Request<BuffersSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_last_offset(
            request.number as usize,
            request.offset as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_time(&self, request: Request<BufferGroupTime>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_time(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_earlier(&self, request: Request<BufferGroupEarlier>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_earlier(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_later(&self, request: Request<BufferGroupLater>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_later(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_range(&self, request: Request<BufferGroupRange>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_range(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_number_before(&self, request: Request<BufferGroupNumber>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_number_before(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.number as usize,
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_by_number_after(&self, request: Request<BufferGroupNumber>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_by_number_after(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.number as usize,
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn read_buffer_group_first(&self, request: Request<BufferGroupSelector>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_group_first(
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn read_buffer_group_last(&self, request: Request<BufferGroupSelector>)
        -> Result<Response<BufferReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_group_last(
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferReadResponse { result }))
    }

    async fn list_buffer_group_first(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_first(
            request.number as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_first_offset(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_first_offset(
            request.number as usize,
            request.offset as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_last(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_last(
            request.number as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn list_buffer_group_last_offset(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<BufferListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_last_offset(
            request.number as usize,
            request.offset as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferListResponse { results }))
    }

    async fn read_buffer_set(&self, request: Request<BufferSetTime>)
        -> Result<Response<BufferSetReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_set(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferSetReadResponse { result }))
    }

    async fn list_buffer_set_by_time(&self, request: Request<BufferSetTime>)
        -> Result<Response<BufferSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_set_by_time(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferSetListResponse { results }))
    }

    async fn list_buffer_set_by_earlier(&self, request: Request<BufferSetEarlier>)
        -> Result<Response<BufferSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_set_by_earlier(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferSetListResponse { results }))
    }

    async fn list_buffer_set_by_later(&self, request: Request<BufferSetLater>)
        -> Result<Response<BufferSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_set_by_later(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferSetListResponse { results }))
    }

    async fn list_buffer_set_by_range(&self, request: Request<BufferSetRange>)
        -> Result<Response<BufferSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_set_by_range(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferSetListResponse { results }))
    }

    async fn create_buffer(&self, request: Request<BufferSchema>)
        -> Result<Response<BufferCreateResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.create_buffer(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            &ArrayDataValue::from_bytes(
                &request.data_bytes,
                request.data_type.into_iter().map(|e| DataType::from(e)).collect::<Vec<DataType>>().as_slice()
            ).to_vec(),
            Some(request.tag as i16)
        ).await;
        let id = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCreateResponse { id }))
    }

    async fn create_buffer_multiple(&self, request: Request<BufferMultipleSchema>)
        -> Result<Response<BufferCreateMultipleResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_BUFFER)?;
        let request = request.into_inner();
        let (device_ids, model_ids, timestamps, data_vec, tags): (Vec<Uuid>, Vec<Uuid>, Vec<DateTime<Utc>>, Vec<Vec<DataValue>>, Vec<i16>) 
            = request.schemas.into_iter().map(|r| {(
                Uuid::from_slice(&r.device_id).unwrap_or_default(),
                Uuid::from_slice(&r.model_id).unwrap_or_default(),
                Utc.timestamp_nanos(&r.timestamp * 1000),
                ArrayDataValue::from_bytes(
                    &r.data_bytes,
                    &r.data_type.iter().map(|&e| DataType::from(e)).collect::<Vec<DataType>>().as_slice()
                ).to_vec(),
                r.tag as i16
            )}).collect();
        let data_multiple: Vec<&[DataValue]> = data_vec.iter().map(|d| d.as_slice()).collect();
        let result = self.resource_db.create_buffer_multiple(
            &device_ids,
            &model_ids,
            &timestamps,
            &data_multiple,
            Some(&tags)
        ).await;
        let ids = match result {
            Ok(value) => value,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCreateMultipleResponse { ids }))
    }

    async fn update_buffer(&self, request: Request<BufferUpdate>)
        -> Result<Response<BufferChangeResponse>, Status>
    {
        self.validate(request.extensions(), UPDATE_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.update_buffer(
            request.id,
            request.data_bytes.map(|s| {
                ArrayDataValue::from_bytes(
                    &s,
                    request.data_type.into_iter().map(|e| DataType::from(e)).collect::<Vec<DataType>>().as_slice()
                ).to_vec()
            }).as_deref(),
            request.tag.map(|t| t as i16)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferChangeResponse { }))
    }

    async fn update_buffer_by_time(&self, request: Request<BufferUpdateTime>)
        -> Result<Response<BufferChangeResponse>, Status>
    {
        self.validate(request.extensions(), UPDATE_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.update_buffer_by_time(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.data_bytes.map(|s| {
                ArrayDataValue::from_bytes(
                    &s,
                    request.data_type.into_iter().map(|e| DataType::from(e)).collect::<Vec<DataType>>().as_slice()
                ).to_vec()
            }).as_deref(),
            request.tag.map(|t| t as i16)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferChangeResponse { }))
    }

    async fn delete_buffer(&self, request: Request<BufferId>)
        -> Result<Response<BufferChangeResponse>, Status>
    {
        self.validate(request.extensions(), DELETE_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.delete_buffer(request.id).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferChangeResponse { }))
    }

    async fn delete_buffer_by_time(&self, request: Request<BufferTime>)
        -> Result<Response<BufferChangeResponse>, Status>
    {
        self.validate(request.extensions(), DELETE_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.delete_buffer_by_time(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferChangeResponse { }))
    }

    async fn read_buffer_timestamp(&self, request: Request<BufferTime>)
        -> Result<Response<TimestampReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_timestamp(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamp = match result {
            Ok(value) => value.timestamp_micros(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampReadResponse { timestamp }))
    }

    async fn list_buffer_timestamp_by_earlier(&self, request: Request<BufferEarlier>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_timestamp_by_earlier(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_timestamp_by_later(&self, request: Request<BufferLater>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_timestamp_by_later(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_timestamp_by_range(&self, request: Request<BufferRange>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_timestamp_by_range(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_timestamp_first(&self, request: Request<BuffersSelector>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_timestamp_first(
            request.number as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_timestamp_last(&self, request: Request<BuffersSelector>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_timestamp_last(
            request.number as usize,
            request.device_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.model_id.map(|x| Uuid::from_slice(&x).unwrap_or_default()),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn read_buffer_group_timestamp(&self, request: Request<BufferGroupTime>)
        -> Result<Response<TimestampReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.read_buffer_group_timestamp(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamp = match result {
            Ok(value) => value.timestamp_micros(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampReadResponse { timestamp }))
    }

    async fn list_buffer_group_timestamp_by_earlier(&self, request: Request<BufferGroupEarlier>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_timestamp_by_earlier(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_group_timestamp_by_later(&self, request: Request<BufferGroupLater>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_timestamp_by_later(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_group_timestamp_by_range(&self, request: Request<BufferGroupRange>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_timestamp_by_range(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_group_timestamp_first(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_timestamp_first(
            request.number as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn list_buffer_group_timestamp_last(&self, request: Request<BuffersGroupSelector>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.list_buffer_group_timestamp_last(
            request.number as usize,
            Some(&request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            Some(&request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>()),
            request.tag.map(|t| t as i16)
        ).await;
        let timestamps = match result {
            Ok(value) => value.into_iter().map(|t| t.timestamp_micros()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(TimestampListResponse { timestamps }))
    }

    async fn count_buffer(&self, request: Request<BufferTime>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_by_earlier(&self, request: Request<BufferEarlier>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_by_earlier(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_by_later(&self, request: Request<BufferLater>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_by_later(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_by_range(&self, request: Request<BufferRange>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_by_range(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_group(&self, request: Request<BufferGroupTime>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_group(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_group_by_earlier(&self, request: Request<BufferGroupEarlier>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_group_by_earlier(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_group_by_later(&self, request: Request<BufferGroupLater>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_group_by_later(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

    async fn count_buffer_group_by_range(&self, request: Request<BufferGroupRange>)
        -> Result<Response<BufferCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_BUFFER)?;
        let request = request.into_inner();
        let result = self.resource_db.count_buffer_group_by_range(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(BufferCountResponse { count }))
    }

}

impl AccessValidator for BufferServer {

    fn token_key(&self) -> Vec<u8> {
        self.token_key.clone()
    }

    fn accesses(&self) -> Vec<AccessSchema> {
        self.accesses.clone()
    }

}
