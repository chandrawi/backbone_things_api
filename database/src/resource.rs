pub(crate) mod _schema;
pub(crate) mod _row;
pub mod model;
pub mod device;
pub mod group;
pub mod set;
pub mod data;
pub mod buffer;
pub mod slice;

use sqlx::{Pool, Error};
use sqlx::postgres::{Postgres, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use std::slice::from_ref;
use crate::common::type_value::{DataType, DataValue, ArrayDataValue};
use _schema::{
    ModelSchema, ModelConfigSchema, TagSchema, 
    DeviceSchema, DeviceConfigSchema, GatewaySchema, GatewayConfigSchema, TypeSchema,
    GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema, SetSchema, SetTemplateSchema,
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema, SliceSchema, SliceSetSchema
};
use device::DeviceKind;
use group::GroupKind;
use data::DataSelector;
use buffer::BufferSelector;
use slice::SliceSelector;

#[derive(Debug, Clone)]
pub struct Resource {
    pool: Pool<Postgres>
}

impl Resource {

    pub async fn new(host: &str, username: &str, password: &str, database: &str) -> Self {
        let url = format!("postgres://{}:{}@{}/{}", username, password, host, database);
        Resource::new_with_url(&url).await
    }

    pub async fn new_with_url(url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Resource { pool }
    }

    pub fn new_with_pool(pool: &Pool<Postgres>) -> Self {
        Resource { pool: pool.to_owned() }
    }

    pub async fn read_model(&self, id: Uuid)
        -> Result<ModelSchema, Error>
    {
        let qs = model::select_model(Some(id), None, None, None, None);
        qs.fetch_model_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ModelSchema>, Error>
    {
        let qs = model::select_model(None, Some(ids), None, None, None);
        qs.fetch_model_schema(&self.pool).await
    }

    pub async fn list_model_by_type(&self, type_id: Uuid)
        -> Result<Vec<ModelSchema>, Error>
    {
        let qs = model::select_model(None, None, Some(type_id), None, None);
        qs.fetch_model_schema(&self.pool).await
    }

    pub async fn list_model_by_name(&self, name: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        let qs = model::select_model(None, None, None, Some(name), None);
        qs.fetch_model_schema(&self.pool).await
    }

    pub async fn list_model_by_category(&self, category: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        let qs = model::select_model(None, None, None, None, Some(category));
        qs.fetch_model_schema(&self.pool).await
    }

    pub async fn list_model_option(&self, type_id: Option<Uuid>, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<ModelSchema>, Error>
    {
        let qs = model::select_model(None, None, type_id, name, category);
        qs.fetch_model_schema(&self.pool).await
    }

    pub async fn create_model(&self, id: Uuid, data_type: &[DataType], category: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = model::insert_model(id, data_type, category, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_model(&self, id: Uuid, data_type: Option<&[DataType]>, category: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = model::update_model(id, data_type, category, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_model(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = model::delete_model(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_model_config(&self, id: i32)
        -> Result<ModelConfigSchema, Error>
    {
        let qs = model::select_model_config(Some(id), None);
        qs.fetch_model_config_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_model_config_by_model(&self, model_id: Uuid)
        -> Result<Vec<ModelConfigSchema>, Error>
    {
        let qs = model::select_model_config(None, Some(model_id));
        qs.fetch_model_config_schema(&self.pool).await
    }

    pub async fn create_model_config(&self, model_id: Uuid, index: i32, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        let qs = model::insert_model_config(model_id, index, name, value, category);
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_model_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        let qs = model::update_model_config(id, name, value, category);
        qs.execute(&self.pool).await
    }

    pub async fn delete_model_config(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = model::delete_model_config(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_tag(&self, model_id: Uuid, tag: i16)
        -> Result<TagSchema, Error>
    {
        let qs = model::select_model_tag(model_id, Some(tag));
        qs.fetch_tag_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_tag_by_model(&self, model_id: Uuid)
        -> Result<Vec<TagSchema>, Error>
    {
        let qs = model::select_model_tag(model_id, None);
        qs.fetch_tag_schema(&self.pool).await
    }

    pub async fn create_tag(&self, model_id: Uuid, tag: i16, name: &str, members: &[i16])
        -> Result<(), Error>
    {
        let qs = model::insert_model_tag(model_id, tag, name, members);
        qs.execute(&self.pool).await
    }

    pub async fn update_tag(&self, model_id: Uuid, tag: i16, name: Option<&str>, members: Option<&[i16]>)
        -> Result<(), Error>
    {
        let qs = model::update_model_tag(model_id, tag, name, members);
        qs.execute(&self.pool).await
    }

    pub async fn delete_tag(&self, model_id: Uuid, tag: i16)
        -> Result<(), Error>
    {
        let qs = model::delete_model_tag(model_id, tag);
        qs.execute(&self.pool).await
    }

    pub async fn read_device(&self, id: Uuid)
        -> Result<DeviceSchema, Error>
    {
        let qs = device::select_device(DeviceKind::Device, Some(id), None, None, None, None, None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_device_by_sn(&self, serial_number: &str)
        -> Result<DeviceSchema, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, Some(serial_number), None, None, None, None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<DeviceSchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, None, Some(ids), None, None, None);
        qs.fetch_device_schema(&self.pool).await
    }

    pub async fn list_device_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, None, None, Some(gateway_id), None, None);
        qs.fetch_device_schema(&self.pool).await
    }

    pub async fn list_device_by_type(&self, type_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, None, None, None, Some(type_id), None);
        qs.fetch_device_schema(&self.pool).await
    }

    pub async fn list_device_by_name(&self, name: &str)
        -> Result<Vec<DeviceSchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, None, None, None, None, Some(name));
        qs.fetch_device_schema(&self.pool).await
    }

    pub async fn list_device_option(&self, gateway_id: Option<Uuid>, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<DeviceSchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Device, None, None, None, gateway_id, type_id, name);
        qs.fetch_device_schema(&self.pool).await
    }

    pub async fn create_device(&self, id: Uuid, gateway_id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = device::insert_device(id, gateway_id, type_id, serial_number, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_device(&self, id: Uuid, gateway_id: Option<Uuid>, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = device::update_device(DeviceKind::Device, id, gateway_id, type_id, serial_number, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_device(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = device::delete_device(DeviceKind::Device, id);
        qs.execute(&self.pool).await
    }

    pub async fn read_gateway(&self, id: Uuid)
        -> Result<GatewaySchema, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, Some(id), None, None, None, None, None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn read_gateway_by_sn(&self, serial_number: &str)
        -> Result<GatewaySchema, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, None, Some(serial_number), None, None, None, None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn list_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GatewaySchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, None, None, Some(ids), None, None, None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_gateway_by_type(&self, type_id: Uuid)
        -> Result<Vec<GatewaySchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, None, None, None, None, Some(type_id), None);
        qs.fetch_device_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_gateway_by_name(&self, name: &str)
        -> Result<Vec<GatewaySchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, None, None, None, None, None, Some(name));
        qs.fetch_device_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_gateway_option(&self, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<GatewaySchema>, Error>
    {
        let qs = device::select_device(DeviceKind::Gateway, None, None, None, None, type_id, name);
        qs.fetch_device_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn create_gateway(&self, id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = device::insert_device(id, id, type_id, serial_number, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_gateway(&self, id: Uuid, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = device::update_device(DeviceKind::Gateway, id, None, type_id, serial_number, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_gateway(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = device::delete_device(DeviceKind::Gateway, id);
        qs.execute(&self.pool).await
    }

    pub async fn read_device_config(&self, id: i32)
        -> Result<DeviceConfigSchema, Error>
    {
        let qs = device::select_device_config(DeviceKind::Device, Some(id), None);
        qs.fetch_device_config_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_device_config_by_device(&self, device_id: Uuid)
        -> Result<Vec<DeviceConfigSchema>, Error>
    {
        let qs = device::select_device_config(DeviceKind::Device, None, Some(device_id));
        qs.fetch_device_config_schema(&self.pool).await
    }

    pub async fn create_device_config(&self, device_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        let qs = device::insert_device_config(device_id, name, value, category);
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_device_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        let qs = device::update_device_config(id, name, value, category);
        qs.execute(&self.pool).await
    }

    pub async fn delete_device_config(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = device::delete_device_config(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_gateway_config(&self, id: i32)
        -> Result<GatewayConfigSchema, Error>
    {
        let qs = device::select_device_config(DeviceKind::Gateway, Some(id), None);
        qs.fetch_device_config_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn list_gateway_config_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<GatewayConfigSchema>, Error>
    {
        let qs = device::select_device_config(DeviceKind::Gateway, None, Some(gateway_id));
        qs.fetch_device_config_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn create_gateway_config(&self, gateway_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        let qs = device::insert_device_config(gateway_id, name, value, category);
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_gateway_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        let qs = device::update_device_config(id, name, value, category);
        qs.execute(&self.pool).await
    }

    pub async fn delete_gateway_config(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = device::delete_device_config(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_type(&self, id: Uuid)
        -> Result<TypeSchema, Error>
    {
        let qs = device::select_device_type(Some(id), None, None);
        qs.fetch_type_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_type_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<TypeSchema>, Error>
    {
        let qs = device::select_device_type(None, Some(ids), None);
        qs.fetch_type_schema(&self.pool).await
    }

    pub async fn list_type_by_name(&self, name: &str)
        -> Result<Vec<TypeSchema>, Error>
    {
        let qs = device::select_device_type(None, None, Some(name));
        qs.fetch_type_schema(&self.pool).await
    }

    pub async fn list_type_option(&self, name: Option<&str>)
        -> Result<Vec<TypeSchema>, Error>
    {
        let qs = device::select_device_type(None, None, name);
        qs.fetch_type_schema(&self.pool).await
    }

    pub async fn create_type(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = device::insert_device_type(id, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_type(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = device::update_device_type(id, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_type(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = device::delete_device_type(id);
        qs.execute(&self.pool).await
    }

    pub async fn add_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        let qs = device::insert_device_type_model(id, model_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        let qs = device::delete_device_type_model(id, model_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_group_model(&self, id: Uuid)
        -> Result<GroupModelSchema, Error>
    {
        let qs = group::select_group(GroupKind::Model, Some(id), None, None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn list_group_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupModelSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Model, None, Some(ids), None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_model_by_name(&self, name: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Model, None, None, Some(name), None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_model_by_category(&self, category: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Model, None, None, None, Some(category));
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_model_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Model, None, None, name, category);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn create_group_model(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = group::insert_group(GroupKind::Model, id, name, category, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_group_model(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = group::update_group(GroupKind::Model, id, name, category, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_group_model(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group(GroupKind::Model, id);
        qs.execute(&self.pool).await
    }

    pub async fn add_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::insert_group_map(GroupKind::Model, id, model_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group_map(GroupKind::Model, id, model_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_group_device(&self, id: Uuid)
        -> Result<GroupDeviceSchema, Error>
    {
        let qs = group::select_group(GroupKind::Device, Some(id), None, None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn list_group_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Device, None, Some(ids), None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_device_by_name(&self, name: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Device, None, None, Some(name), None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_device_by_category(&self, category: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Device, None, None, None, Some(category));
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_device_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        let qs = group::select_group(GroupKind::Device, None, None, name, category);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn create_group_device(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = group::insert_group(GroupKind::Device, id, name, category, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_group_device(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = group::update_group(GroupKind::Device, id, name, category, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_group_device(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group(GroupKind::Device, id);
        qs.execute(&self.pool).await
    }

    pub async fn add_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::insert_group_map(GroupKind::Device, id, device_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group_map(GroupKind::Device, id, device_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_group_gateway(&self, id: Uuid)
        -> Result<GroupGatewaySchema, Error>
    {
        let qs = group::select_group(GroupKind::Gateway, Some(id), None, None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().next().map(|s| s.into()).ok_or(Error::RowNotFound)
    }

    pub async fn list_group_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        let qs = group::select_group(GroupKind::Gateway, None, Some(ids), None, None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_gateway_by_name(&self, name: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        let qs = group::select_group(GroupKind::Gateway, None, None, Some(name), None);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_gateway_by_category(&self, category: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        let qs = group::select_group(GroupKind::Gateway, None, None, None, Some(category));
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn list_group_gateway_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        let qs = group::select_group(GroupKind::Gateway, None, None, name, category);
        qs.fetch_group_schema(&self.pool).await?.into_iter().map(|s| Ok(s.into())).collect()
    }

    pub async fn create_group_gateway(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = group::insert_group(GroupKind::Gateway, id, name, category, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_group_gateway(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = group::update_group(GroupKind::Gateway, id, name, category, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_group_gateway(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group(GroupKind::Gateway, id);
        qs.execute(&self.pool).await
    }

    pub async fn add_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::insert_group_map(GroupKind::Gateway, id, gateway_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Error>
    {
        let qs = group::delete_group_map(GroupKind::Gateway, id, gateway_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_set(&self, id: Uuid)
        -> Result<SetSchema, Error>
    {
        let qs = set::select_set(Some(id), None, None, None);
        qs.fetch_set_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_set_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetSchema>, Error>
    {
        let qs = set::select_set(None, Some(ids), None, None);
        qs.fetch_set_schema(&self.pool).await
    }

    pub async fn list_set_by_template(&self, template_id: Uuid)
        -> Result<Vec<SetSchema>, Error>
    {
        let qs = set::select_set(None, None, Some(template_id), None);
        qs.fetch_set_schema(&self.pool).await
    }

    pub async fn list_set_by_name(&self, name: &str)
        -> Result<Vec<SetSchema>, Error>
    {
        let qs = set::select_set(None, None, None, Some(name));
        qs.fetch_set_schema(&self.pool).await
    }

    pub async fn list_set_option(&self, template_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<SetSchema>, Error>
    {
        let qs = set::select_set(None, None, template_id, name);
        qs.fetch_set_schema(&self.pool).await
    }

    pub async fn create_set(&self, id: Uuid, template_id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = set::insert_set(id, template_id, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_set(&self, id: Uuid, template_id: Option<Uuid>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = set::update_set(id, template_id, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_set(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = set::delete_set(id);
        qs.execute(&self.pool).await
    }

    pub async fn add_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Error>
    {
        // get members of the set then calculate new data position and data number
        let qs = set::read_set_members(id);
        let members = qs.fetch_set_members(&self.pool).await?;
        let position = members.iter().fold(0, |acc, e| acc + e.data_index.len());
        let number = position + data_index.len();
        let qs = set::insert_set_member(id, device_id, model_id, data_index, position as i16, number as i16);
        qs.execute(&self.pool).await?;
        // update data number of all set members
        let qs = set::update_set_position_number(id, device_id, model_id, None, Some(number as i16));
        qs.execute(&self.pool).await
    }

    pub async fn remove_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        // get members of the set then get index position of deleted set member
        let qs = set::read_set_members(id);
        let members = qs.fetch_set_members(&self.pool).await?;
        let index = members.iter().position(|e| e.device_id == device_id && e.model_id == model_id);
        let qs = set::delete_set_member(id, device_id, model_id);
        qs.execute(&self.pool).await?;
        if let Some(idx) = index {
            // calculate data number then update data number of all set members
            let number = members.iter().fold(0, |acc, e| acc + e.data_index.len()) - members[idx].data_index.len();
            let qs = set::update_set_position_number(id, device_id, model_id, None, Some(number as i16));
            qs.execute(&self.pool).await?;
            // update data position of members with index position after deleted set member
            let mut position = 0;
            for (i, member) in members.iter().enumerate() {
                if i > idx {
                    let qs = set::update_set_position_number(id, member.device_id, member.model_id, Some(position), None);
                    qs.execute(&self.pool).await?;
                }
                position += member.data_index.len() as i16;
            }
        }
        Ok(())
    }

    pub async fn swap_set_member(&self, id: Uuid, device_id_1: Uuid, model_id_1: Uuid, device_id_2: Uuid, model_id_2: Uuid)
        -> Result<(), Error>
    {
        // get members of the set then get index positions
        let qs = set::read_set_members(id);
        let mut members = qs.fetch_set_members(&self.pool).await?;
        let index_1 = members.iter().position(|e| e.device_id == device_id_1 && e.model_id == model_id_1);
        let index_2 = members.iter().position(|e| e.device_id == device_id_2 && e.model_id == model_id_2);
        // swap position index
        if let (Some(i1), Some(i2)) = (index_1, index_2) {
            members.swap(i1, i2);
            // update data position of members
            let mut position = 0;
            for (i, member) in members.iter().enumerate() {
                if i >= i1 || i >= i2 {
                    let qs = set::update_set_position_number(id, member.device_id, member.model_id, Some(position), None);
                    qs.execute(&self.pool).await?;
                }
                position += member.data_index.len() as i16;
            }
        }
        Ok(())
    }

    pub async fn read_set_template(&self, id: Uuid)
        -> Result<SetTemplateSchema, Error>
    {
        let qs = set::select_set_template(Some(id), None, None);
        qs.fetch_set_template_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_set_template_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        let qs = set::select_set_template(None, Some(ids), None);
        qs.fetch_set_template_schema(&self.pool).await
    }

    pub async fn list_set_template_by_name(&self, name: &str)
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        let qs = set::select_set_template(None, None, Some(name));
        qs.fetch_set_template_schema(&self.pool).await
    }

    pub async fn list_set_template_option(&self, name: Option<&str>)
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        let qs = set::select_set_template(None, None, name);
        qs.fetch_set_template_schema(&self.pool).await
    }

    pub async fn create_set_template(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        let qs = set::insert_set_template(id, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_set_template(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = set::update_set_template(id, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_set_template(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = set::delete_set_template(id);
        qs.execute(&self.pool).await
    }

    pub async fn add_set_template_member(&self, id: Uuid, type_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Error>
    {
        // get members of the set template then calculate new template index
        let qs = set::read_set_template_members(id);
        let members = qs.fetch_set_template_members(&self.pool).await?;
        let new_index = members.len() as i16;
        let qs = set::insert_set_template_member(id, type_id, model_id, data_index, new_index);
        qs.execute(&self.pool).await
    }

    pub async fn remove_set_template_member(&self, id: Uuid, index: usize)
        -> Result<(), Error>
    {
        // get members of the set template
        let qs = set::read_set_template_members(id);
        let members = qs.fetch_set_template_members(&self.pool).await?;
        let qs = set::delete_set_template_member(id, index as i16);
        qs.execute(&self.pool).await?;
        // update template index after deleted member
        for i in 0..members.len() {
            if i > index {
                let qs = set::update_set_template_index(id, i as i16, i as i16 - 1);
                qs.execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    pub async fn swap_set_template_member(&self, id: Uuid, index_1: usize, index_2: usize)
        -> Result<(), Error>
    {
        // update data position and data number
        let qs = set::update_set_template_index(id, index_1 as i16, i16::MAX);
        qs.execute(&self.pool).await?;
        let qs = set::update_set_template_index(id, index_2 as i16, index_1 as i16);
        qs.execute(&self.pool).await?;
        let qs = set::update_set_template_index(id, i16::MAX, index_2 as i16);
        qs.execute(&self.pool).await
    }

    async fn get_tags(&self, model_ids: &[Uuid], tag: Option<i16>) -> Option<Vec<i16>>
    {
        match tag {
            Some(tag) => {
                let qs = model::select_tag_members(model_ids, tag);
                Some(qs.fetch_tag_members(&self.pool, tag).await)
            },
            None => None
        }
    }

    async fn get_tags_set(&self, set_id: Uuid, tag: Option<i16>) -> Option<Vec<i16>>
    {
        match tag {
            Some(tag) => {
                let qs = model::select_tag_members_set(set_id, tag);
                Some(qs.fetch_tag_members(&self.pool, tag).await)
            },
            None => None
        }
    }

    async fn get_tags_option(&self, model_ids: Option<&[Uuid]>, tag: Option<i16>) -> Option<Vec<i16>>
    {
        match (model_ids, tag) {
            (Some(model_ids), Some(tag)) => {
                let qs = model::select_tag_members(model_ids, tag);
                Some(qs.fetch_tag_members(&self.pool, tag).await)
            },
            _ => None
        }
    }

    pub async fn read_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSchema, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_data_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Earlier(earlier);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Later(later);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Range(begin, end);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::NumberBefore(before, number);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::NumberAfter(after, number);
        let qs = data::select_data(selector, &[device_id], &[model_id], tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Earlier(earlier);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Later(later);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Range(begin, end);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::NumberBefore(before, number);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn list_data_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::NumberAfter(after, number);
        let qs = data::select_data(selector, device_ids, model_ids, tags);
        qs.fetch_data_schema(&self.pool).await
    }

    pub async fn read_data_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSetSchema, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data_set(selector, set_id, tags);
        qs.fetch_data_set_schema(&self.pool, set_id).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_data_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data_set(selector, set_id, tags);
        qs.fetch_data_set_schema(&self.pool, set_id).await
    }

    pub async fn list_data_set_by_earlier(&self, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = DataSelector::Earlier(earlier);
        let qs = data::select_data_set(selector, set_id, tags);
        qs.fetch_data_set_schema(&self.pool, set_id).await
    }

    pub async fn list_data_set_by_later(&self, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = DataSelector::Later(later);
        let qs = data::select_data_set(selector, set_id, tags);
        qs.fetch_data_set_schema(&self.pool, set_id).await
    }

    pub async fn list_data_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = DataSelector::Range(begin, end);
        let qs = data::select_data_set(selector, set_id, tags);
        qs.fetch_data_set_schema(&self.pool, set_id).await
    }

    pub async fn create_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<(), Error>
    {
        // get data types then try to convert the data
        let qs = data::select_data_types(&[model_id]);
        let types = qs.fetch_data_types(&self.pool).await?.into_iter().next().unwrap_or_default();
        let data = ArrayDataValue::from_vec(data).convert(&types)
            .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?;
        let qs = data::insert_data(device_id, model_id, timestamp, &data.to_vec(), tag);
        qs.execute(&self.pool).await
    }

    pub async fn create_data_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<(), Error>
    {
        // get the number of data and check if all the arrays has the same length
        let number = data.len();
        let numbers = vec![device_ids.len(), model_ids.len(), timestamps.len()];
        if number == 0 || numbers.into_iter().any(|n| n != number) {
            return Err(Error::InvalidArgument(EMPTY_LENGTH_UNMATCH.to_string()))
        }
        // get data types array from unique model id then try to convert the data array
        let mut model_ids_unique = model_ids.to_vec();
        model_ids_unique.sort();
        model_ids_unique.dedup();
        let qs = data::select_data_types(&model_ids_unique);
        let types_vec = qs.fetch_data_types(&self.pool).await?;
        let mut data_vec = Vec::new();
        for i in 0..number {
            let index = model_ids_unique.iter().position(|&id_unique| id_unique == model_ids[i]).unwrap_or_default();
            let types = types_vec.get(index).unwrap_or(&Vec::new()).to_vec();
            let adv = ArrayDataValue::from_vec(data[i]).convert(&types)
                .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?;
            data_vec.push(adv.to_vec());
        }
        let data_slice: Vec<&[DataValue]> = data_vec.iter().map(|d| d.as_slice()).collect();
        let qs = data::insert_data_multiple(device_ids, model_ids, timestamps, &data_slice, tags);
        qs.execute(&self.pool).await
    }

    pub async fn delete_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Error>
    {
        let qs = data::delete_data(device_id, model_id, timestamp, tag);
        qs.execute(&self.pool).await
    }

    pub async fn read_data_timestamp(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data_timestamp(selector, &[device_id], &[model_id], tags);
        qs.fetch_timestamp(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_data_timestamp_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Earlier(earlier);
        let qs = data::select_data_timestamp(selector, &[device_id], &[model_id], tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_data_timestamp_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Later(later);
        let qs = data::select_data_timestamp(selector, &[device_id], &[model_id], tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_data_timestamp_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = DataSelector::Range(begin, end);
        let qs = data::select_data_timestamp(selector, &[device_id], &[model_id], tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn read_data_group_timestamp(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Time(timestamp);
        let qs = data::select_data_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_data_group_timestamp_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Earlier(earlier);
        let qs = data::select_data_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_data_group_timestamp_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Later(later);
        let qs = data::select_data_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_data_group_timestamp_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = DataSelector::Range(begin, end);
        let qs = data::select_data_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn count_data(&self, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = data::count_data(DataSelector::Time(DateTime::default()), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = data::count_data(DataSelector::Earlier(earlier), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = data::count_data(DataSelector::Later(later), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = data::count_data(DataSelector::Range(begin, end), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_group(&self, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = data::count_data(DataSelector::Time(DateTime::default()), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = data::count_data(DataSelector::Earlier(earlier), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = data::count_data(DataSelector::Later(later), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = data::count_data(DataSelector::Range(begin, end), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn read_buffer(&self, id: i32)
        -> Result<BufferSchema, Error>
    {
        let qs = buffer::select_buffer(BufferSelector::None, Some(&[id]), None, None, None);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_by_ids(&self, ids: &[i32])
        -> Result<Vec<BufferSchema>, Error>
    {
        let qs = buffer::select_buffer(BufferSelector::None, Some(ids), None, None, None);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Earlier(earlier);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Later(later);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::NumberBefore(before, number);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::NumberAfter(after, number);
        let qs = buffer::select_buffer(selector, None, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn read_buffer_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::First(1, 0);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::Last(1, 0);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::First(number, 0);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_first_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::First(number, offset);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::Last(number, 0);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_last_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::Last(number, offset);
        let qs = buffer::select_buffer(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Earlier(earlier);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Later(later);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::NumberBefore(before, number);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::NumberAfter(after, number);
        let qs = buffer::select_buffer(selector, None, Some(device_ids), Some(model_ids), tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn read_buffer_group_first(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::First(1, 0);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_group_last(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::Last(1, 0);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_group_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::First(number, 0);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_first_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::First(number, offset);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::Last(number, 0);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn list_buffer_group_last_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::Last(number, offset);
        let qs = buffer::select_buffer(selector, None, device_ids, model_ids, tags);
        qs.fetch_buffer_schema(&self.pool).await
    }

    pub async fn read_buffer_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSetSchema, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer_set(selector, set_id, tags);
        qs.fetch_buffer_set_schema(&self.pool, set_id).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer_set(selector, set_id, tags);
        qs.fetch_buffer_set_schema(&self.pool, set_id).await
    }

    pub async fn list_buffer_set_by_earlier(&self, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = BufferSelector::Earlier(earlier);
        let qs = buffer::select_buffer_set(selector, set_id, tags);
        qs.fetch_buffer_set_schema(&self.pool, set_id).await
    }

    pub async fn list_buffer_set_by_later(&self, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = BufferSelector::Later(later);
        let qs = buffer::select_buffer_set(selector, set_id, tags);
        qs.fetch_buffer_set_schema(&self.pool, set_id).await
    }

    pub async fn list_buffer_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let tags = self.get_tags_set(set_id, tag).await;
        let selector = BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer_set(selector, set_id, tags);
        qs.fetch_buffer_set_schema(&self.pool, set_id).await
    }

    pub async fn create_buffer(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<i32, Error>
    {
        // get data types then try to convert the data
        let qs = data::select_data_types(&[model_id]);
        let types = qs.fetch_data_types(&self.pool).await?.into_iter().next().unwrap_or_default();
        let data = ArrayDataValue::from_vec(data).convert(&types)
            .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?.to_vec();
        let qs = buffer::insert_buffer(device_id, model_id, timestamp, &data, tag);
        qs.fetch_id(&self.pool).await
    }

    pub async fn create_buffer_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<Vec<i32>, Error>
    {
        // get the number of data and check if all the arrays has the same length
        let number = data.len();
        let numbers = vec![device_ids.len(), model_ids.len(), timestamps.len()];
        if number == 0 || numbers.into_iter().any(|n| n != number) {
            return Err(Error::InvalidArgument(EMPTY_LENGTH_UNMATCH.to_string()))
        }
        // get data types array from unique model id then try to convert the data array
        let mut model_ids_unique = model_ids.to_vec();
        model_ids_unique.sort();
        model_ids_unique.dedup();
        let qs = data::select_data_types(&model_ids_unique);
        let types_vec = qs.fetch_data_types(&self.pool).await?;
        let mut data_vec = Vec::new();
        for i in 0..number {
            let index = model_ids_unique.iter().position(|&id_unique| id_unique == model_ids[i]).unwrap_or_default();
            let types = types_vec.get(index).unwrap_or(&Vec::new()).to_vec();
            let adv = ArrayDataValue::from_vec(data[i]).convert(&types)
                .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?;
            data_vec.push(adv.to_vec());
        }
        let data_slice: Vec<&[DataValue]> = data_vec.iter().map(|d| d.as_slice()).collect();
        let qs = buffer::insert_buffer_multiple(device_ids, model_ids, timestamps, &data_slice, tags);
        let id = qs.fetch_id(&self.pool).await?;
        Ok((id..id+number as i32).collect())
    }

    pub async fn update_buffer(&self, id: i32, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Error>
    {
        // get data types then try to convert the data
        let qs = buffer::select_buffer_types(id);
        let types = qs.fetch_buffer_types(&self.pool).await?;
        let data = match data {
            Some(d) => Some(ArrayDataValue::from_vec(d).convert(&types)
                .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?.to_vec()),
            None => None
        };
        let data = data.as_ref().map(|d| d.as_slice());
        let qs = buffer::update_buffer(Some(id), None, None, None, data, tag);
        qs.execute(&self.pool).await
    }

    pub async fn update_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Error>
    {
        // get data types then try to convert the data
        let qs = data::select_data_types(&[model_id]);
        let types = qs.fetch_buffer_types(&self.pool).await?;
        let data = match data {
            Some(d) => Some(ArrayDataValue::from_vec(d).convert(&types)
                .ok_or(Error::InvalidArgument(String::from(DATA_TYPE_UNMATCH)))?.to_vec()),
            None => None
        };
        let data = data.as_ref().map(|d| d.as_slice());
        let qs = buffer::update_buffer(None, Some(device_id), Some(model_id), Some(timestamp), data, tag);
        qs.execute(&self.pool).await
    }

    pub async fn delete_buffer(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = buffer::delete_buffer(Some(id), None, None, None, None);
        qs.execute(&self.pool).await
    }

    pub async fn delete_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Error>
    {
        let qs = buffer::delete_buffer(None, Some(device_id), Some(model_id), Some(timestamp), tag);
        qs.execute(&self.pool).await
    }

    pub async fn read_buffer_timestamp(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer_timestamp(selector, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_timestamp(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_timestamp_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Earlier(earlier);
        let qs = buffer::select_buffer_timestamp(selector, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_timestamp_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Later(later);
        let qs = buffer::select_buffer_timestamp(selector, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_timestamp_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let selector = BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer_timestamp(selector, Some(&[device_id]), Some(&[model_id]), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_timestamp_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::First(number, 0);
        let qs = buffer::select_buffer_timestamp(selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_timestamp_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags_option(model_id.as_ref().map(|id| from_ref(id)), tag).await;
        let selector = BufferSelector::Last(number, 0);
        let qs = buffer::select_buffer_timestamp(selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn read_buffer_group_timestamp(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer_timestamp(selector, Some(device_ids), Some(model_ids), tags);
        qs.fetch_timestamp(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_group_timestamp_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Earlier(earlier);
        let qs = buffer::select_buffer_timestamp(selector, Some(device_ids), Some(model_ids), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_group_timestamp_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Later(later);
        let qs = buffer::select_buffer_timestamp(selector, Some(device_ids), Some(model_ids), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_group_timestamp_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let selector = BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer_timestamp(selector, Some(device_ids), Some(model_ids), tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_group_timestamp_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::First(number, 0);
        let qs = buffer::select_buffer_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await
    }

    pub async fn list_buffer_group_timestamp_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let tags = self.get_tags_option(model_ids, tag).await;
        let selector = BufferSelector::Last(number, 0);
        let qs = buffer::select_buffer_timestamp(selector, device_ids, model_ids, tags);
        qs.fetch_timestamp(&self.pool).await
        
    }

    pub async fn count_buffer(&self, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = buffer::count_buffer(BufferSelector::None, &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = buffer::count_buffer(BufferSelector::Earlier(earlier), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = buffer::count_buffer(BufferSelector::Later(later), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(&[model_id], tag).await;
        let qs = buffer::count_buffer(BufferSelector::Range(begin, end), &[device_id], &[model_id], tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_group(&self, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = buffer::count_buffer(BufferSelector::None, device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = buffer::count_buffer(BufferSelector::Earlier(earlier), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = buffer::count_buffer(BufferSelector::Later(later), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn count_buffer_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        let tags = self.get_tags(model_ids, tag).await;
        let qs = buffer::count_buffer(BufferSelector::Range(begin, end), device_ids, model_ids, tags);
        qs.fetch_count(&self.pool).await
    }

    pub async fn read_slice(&self, id: i32)
        -> Result<SliceSchema, Error>
    {
        let qs = slice::select_slice(SliceSelector::None, Some(&[id]), None, None, None);
        qs.fetch_slice_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_slice_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSchema>, Error>
    {
        let qs = slice::select_slice(SliceSelector::None, Some(ids), None, None, None);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        let qs = slice::select_slice(selector, None, Some(&[device_id]), Some(&[model_id]), None);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        let qs = slice::select_slice(selector, None, Some(&[device_id]), Some(&[model_id]), None);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        let qs = slice::select_slice(selector, None, None, None, Some(name));
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        let qs = slice::select_slice(selector, None, None, None, Some(name));
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_option(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = match (begin_or_timestamp, end) {
            (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
            (Some(timestamp), None) => SliceSelector::Time(timestamp),
            _ => SliceSelector::None
        };
        let qs = slice::select_slice(selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), name);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        let qs = slice::select_slice(selector, None, Some(device_ids), Some(model_ids), None);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        let qs = slice::select_slice(selector, None, Some(device_ids), Some(model_ids), None);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn list_slice_group_option(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = match (begin_or_timestamp, end) {
            (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
            (Some(timestamp), None) => SliceSelector::Time(timestamp),
            _ => SliceSelector::None
        };
        let qs = slice::select_slice(selector, None, device_ids, model_ids, name);
        qs.fetch_slice_schema(&self.pool).await
    }

    pub async fn create_slice(&self, device_id: Uuid, model_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Error>
    {
        let qs = slice::insert_slice(device_id, model_id, timestamp_begin, timestamp_end, name, description);
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_slice(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = slice::update_slice(id, timestamp_begin, timestamp_end, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_slice(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = slice::delete_slice(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_slice_set(&self, id: i32)
        -> Result<SliceSetSchema, Error>
    {
        let qs = slice::select_slice_set(SliceSelector::None, Some(&[id]), None, None);
        qs.fetch_slice_set_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_slice_set_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let qs = slice::select_slice_set(SliceSelector::None, Some(ids), None, None);
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn list_slice_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        let qs = slice::select_slice_set(selector, None, Some(set_id), None);
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn list_slice_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        let qs = slice::select_slice_set(selector, None, Some(set_id), None);
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn list_slice_set_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        let qs = slice::select_slice_set(selector, None, None, Some(name));
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn list_slice_set_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        let qs = slice::select_slice_set(selector, None, None, Some(name));
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn list_slice_set_option(&self, set_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = match (begin_or_timestamp, end) {
            (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
            (Some(timestamp), None) => SliceSelector::Time(timestamp),
            _ => SliceSelector::None
        };
        let qs = slice::select_slice_set(selector, None, set_id, name);
        qs.fetch_slice_set_schema(&self.pool).await
    }

    pub async fn create_slice_set(&self, set_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Error>
    {
        let qs = slice::insert_slice_set(set_id, timestamp_begin, timestamp_end, name, description);
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_slice_set(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = slice::update_slice_set(id, timestamp_begin, timestamp_end, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_slice_set(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = slice::delete_slice_set(id);
        qs.execute(&self.pool).await
    }

}

const DATA_TYPE_UNMATCH: &str = "The type of input data argument doesn't match with the model";
const EMPTY_LENGTH_UNMATCH: &str = "One or more input array arguments are empty or doesn't have the same length";
