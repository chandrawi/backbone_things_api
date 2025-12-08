use tonic::{Request, Response, Status};
use chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::{Resource, DataValue, DataType, ArrayDataValue};
use crate::proto::resource::data::data_service_server::DataService;
use crate::proto::resource::data::{
    DataSchema, DataMultipleSchema, DataTime, DataEarlier, DataLater, DataRange, DataNumber,
    DataGroupTime, DataGroupEarlier, DataGroupLater, DataGroupRange, DataGroupNumber,
    DataSetTime, DataSetEarlier, DataSetLater, DataSetRange,
    DataReadResponse, DataListResponse, DataChangeResponse, DataSetReadResponse, DataSetListResponse,
    TimestampReadResponse, TimestampListResponse, DataCountResponse
};
use crate::common::validator::{AccessValidator, AccessSchema};
use crate::common::utility::handle_error;

const READ_DATA: &str = "read_data";
const CREATE_DATA: &str = "create_data";
const DELETE_DATA: &str = "delete_data";

#[derive(Debug)]
pub struct DataServer {
    resource_db: Resource,
    token_key: Vec<u8>,
    accesses: Vec<AccessSchema>
}

impl DataServer {
    pub fn new(resource_db: Resource) -> Self {
        Self {
            resource_db,
            token_key: Vec::new(),
            accesses: Vec::new()
        }
    }
    pub fn new_with_validator(resource_db: Resource, token_key: &[u8], accesses: &[AccessSchema]) -> Self {
        const PROCEDURES: &[&str] = &[
            READ_DATA, CREATE_DATA, DELETE_DATA
        ];
        Self {
            resource_db,
            token_key: token_key.to_vec(),
            accesses: Self::construct_accesses(accesses, PROCEDURES)
        }
    }
}

#[tonic::async_trait]
impl DataService for DataServer {

    async fn read_data(&self, request: Request<DataTime>)
        -> Result<Response<DataReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.read_data(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataReadResponse { result }))
    }

    async fn list_data_by_time(&self, request: Request<DataTime>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_time(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_by_earlier(&self, request: Request<DataEarlier>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_earlier(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_by_later(&self, request: Request<DataLater>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_later(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_by_range(&self, request: Request<DataRange>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_range(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_by_number_before(&self, request: Request<DataNumber>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_number_before(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_by_number_after(&self, request: Request<DataNumber>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_by_number_after(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_time(&self, request: Request<DataGroupTime>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_time(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_earlier(&self, request: Request<DataGroupEarlier>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_earlier(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_later(&self, request: Request<DataGroupLater>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_later(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_range(&self, request: Request<DataGroupRange>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_range(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_number_before(&self, request: Request<DataGroupNumber>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_number_before(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn list_data_group_by_number_after(&self, request: Request<DataGroupNumber>)
        -> Result<Response<DataListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_by_number_after(
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
        Ok(Response::new(DataListResponse { results }))
    }

    async fn read_data_set(&self, request: Request<DataSetTime>)
        -> Result<Response<DataSetReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.read_data_set(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let result = match result {
            Ok(value) => Some(value.into()),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataSetReadResponse { result }))
    }

    async fn list_data_set_by_time(&self, request: Request<DataSetTime>)
        -> Result<Response<DataSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_set_by_time(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataSetListResponse { results }))
    }

    async fn list_data_set_by_earlier(&self, request: Request<DataSetEarlier>)
        -> Result<Response<DataSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_set_by_earlier(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataSetListResponse { results }))
    }

    async fn list_data_set_by_later(&self, request: Request<DataSetLater>)
        -> Result<Response<DataSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_set_by_later(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataSetListResponse { results }))
    }

    async fn list_data_set_by_range(&self, request: Request<DataSetRange>)
        -> Result<Response<DataSetListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_set_by_range(
            Uuid::from_slice(&request.set_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.begin * 1000),
            Utc.timestamp_nanos(request.end * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let results = match result {
            Ok(value) => value.into_iter().map(|e| e.into()).collect(),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataSetListResponse { results }))
    }

    async fn create_data(&self, request: Request<DataSchema>)
        -> Result<Response<DataChangeResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.create_data(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            &ArrayDataValue::from_bytes(
                &request.data_bytes,
                &request.data_type.into_iter().map(|e| DataType::from(e)).collect::<Vec<DataType>>()
            ).to_vec(),
            Some(request.tag as i16)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataChangeResponse { }))
    }

    async fn create_data_multiple(&self, request: Request<DataMultipleSchema>)
        -> Result<Response<DataChangeResponse>, Status>
    {
        self.validate(request.extensions(), CREATE_DATA)?;
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
        let result = self.resource_db.create_data_multiple(
            &device_ids,
            &model_ids,
            &timestamps,
            &data_multiple,
            Some(&tags)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataChangeResponse { }))
    }

    async fn delete_data(&self, request: Request<DataTime>)
        -> Result<Response<DataChangeResponse>, Status>
    {
        self.validate(request.extensions(), DELETE_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.delete_data(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.timestamp * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        match result {
            Ok(_) => (),
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataChangeResponse { }))
    }

    async fn read_data_timestamp(&self, request: Request<DataTime>)
        -> Result<Response<TimestampReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.read_data_timestamp(
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

    async fn list_data_timestamp_by_earlier(&self, request: Request<DataEarlier>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_timestamp_by_earlier(
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

    async fn list_data_timestamp_by_later(&self, request: Request<DataLater>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_timestamp_by_later(
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

    async fn list_data_timestamp_by_range(&self, request: Request<DataRange>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_timestamp_by_range(
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

    async fn read_data_group_timestamp(&self, request: Request<DataGroupTime>)
        -> Result<Response<TimestampReadResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.read_data_group_timestamp(
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

    async fn list_data_group_timestamp_by_earlier(&self, request: Request<DataGroupEarlier>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_timestamp_by_earlier(
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

    async fn list_data_group_timestamp_by_later(&self, request: Request<DataGroupLater>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_timestamp_by_later(
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

    async fn list_data_group_timestamp_by_range(&self, request: Request<DataGroupRange>)
        -> Result<Response<TimestampListResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.list_data_group_timestamp_by_range(
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

    async fn count_data(&self, request: Request<DataTime>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_by_earlier(&self, request: Request<DataEarlier>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_by_earlier(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_by_later(&self, request: Request<DataLater>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_by_later(
            Uuid::from_slice(&request.device_id).unwrap_or_default(),
            Uuid::from_slice(&request.model_id).unwrap_or_default(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_by_range(&self, request: Request<DataRange>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_by_range(
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
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_group(&self, request: Request<DataGroupTime>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_group(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_group_by_earlier(&self, request: Request<DataGroupEarlier>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_group_by_earlier(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.earlier * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_group_by_later(&self, request: Request<DataGroupLater>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_group_by_later(
            &request.device_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            &request.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect::<Vec<Uuid>>(),
            Utc.timestamp_nanos(request.later * 1000),
            request.tag.map(|t| t as i16)
        ).await;
        let count = match result {
            Ok(value) => value as u32,
            Err(e) => return Err(handle_error(e))
        };
        Ok(Response::new(DataCountResponse { count }))
    }

    async fn count_data_group_by_range(&self, request: Request<DataGroupRange>)
        -> Result<Response<DataCountResponse>, Status>
    {
        self.validate(request.extensions(), READ_DATA)?;
        let request = request.into_inner();
        let result = self.resource_db.count_data_group_by_range(
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
        Ok(Response::new(DataCountResponse { count }))
    }

}

impl AccessValidator for DataServer {

    fn token_key(&self) -> Vec<u8> {
        self.token_key.clone()
    }

    fn accesses(&self) -> Vec<AccessSchema> {
        self.accesses.clone()
    }

}
