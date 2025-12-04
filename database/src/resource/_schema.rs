use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::common::type_value::{DataType, DataValue, ArrayDataValue};
use bbthings_grpc_proto::resource::{model, device, group, set, data, buffer, slice};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelSchema {
    pub id: Uuid,
    pub category: String,
    pub name: String,
    pub description: String,
    pub data_type: Vec<DataType>,
    pub tags: Vec<TagSchema>,
    pub configs: Vec<Vec<ModelConfigSchema>>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TagSchema {
    pub model_id: Uuid,
    pub tag: i16,
    pub name: String,
    pub members: Vec<i16>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelConfigSchema {
    pub id: i32,
    pub model_id: Uuid,
    pub index: i16,
    pub name: String,
    pub value: DataValue,
    pub category: String
}

impl From<model::ModelSchema> for ModelSchema {
    fn from(value: model::ModelSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            category: value.category,
            name: value.name,
            description: value.description,
            data_type: value.data_type.into_iter().map(|e| DataType::from(e)).collect(),
            tags: value.tags.into_iter().map(|t| t.into()).collect(),
            configs: value.configs.into_iter().map(|e| {
                    e.configs.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl Into<model::ModelSchema> for ModelSchema {
    fn into(self) -> model::ModelSchema {
        model::ModelSchema {
            id: self.id.as_bytes().to_vec(),
            category: self.category,
            name: self.name,
            description: self.description,
            data_type: self.data_type.into_iter().map(|e| e.into()).collect(),
            tags: self.tags.into_iter().map(|t| t.into()).collect(),
            configs: self.configs.into_iter().map(|e| model::ConfigSchemaVec {
                    configs: e.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl From<model::TagSchema> for TagSchema {
    fn from(value: model::TagSchema) -> Self {
        Self {
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            tag: value.tag as i16,
            name: value.name,
            members: value.members.into_iter().map(|v| v as i16).collect()
        }
    }
}

impl Into<model::TagSchema> for TagSchema {
    fn into(self) -> model::TagSchema {
        model::TagSchema {
            model_id: self.model_id.as_bytes().to_vec(),
            tag: self.tag as i32,
            name: self.name,
            members: self.members.into_iter().map(|v| v as i32).collect()
        }
    }
}

impl From<model::ConfigSchema> for ModelConfigSchema {
    fn from(value: model::ConfigSchema) -> Self {
        Self {
            id: value.id,
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            index: value.index as i16,
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes, 
                DataType::from(value.config_type)
            ),
            category: value.category
        }
    }
}

impl Into<model::ConfigSchema> for ModelConfigSchema {
    fn into(self) -> model::ConfigSchema {
        model::ConfigSchema {
            id: self.id,
            model_id: self.model_id.as_bytes().to_vec(),
            index: self.index as i32,
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: self.value.get_type().into(),
            category: self.category
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct ModelSchemaFlat {
    pub id: Uuid,
    pub category: String,
    pub name: String,
    pub description: String,
    pub data_type: Vec<DataType>,
    pub tags: Vec<TagSchema>,
    pub configs: Vec<ModelConfigSchema>
}

impl From<ModelSchemaFlat> for ModelSchema {
    fn from(value: ModelSchemaFlat) -> Self {
        let number = value.data_type.len();
        let mut config_schema_vec: Vec<Vec<ModelConfigSchema>> = (0..number).map(|_| Vec::new()).collect();
        for config in value.configs {
            let index = config.index as usize;
            if index < number {
                config_schema_vec[index].push(config);
            }
        }
        Self {
            id: value.id,
            category: value.category,
            name: value.name,
            description: value.description,
            data_type: value.data_type,
            tags: value.tags,
            configs: config_schema_vec
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceSchema {
    pub id: Uuid,
    pub gateway_id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_: TypeSchema,
    pub configs: Vec<DeviceConfigSchema>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewaySchema {
    pub id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_: TypeSchema,
    pub configs: Vec<GatewayConfigSchema>
}

impl From<DeviceSchema> for GatewaySchema {
    fn from(value: DeviceSchema) -> Self {
        Self {
            id: value.gateway_id,
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_: value.type_,
            configs: value.configs.into_iter().map(|el| el.into()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TypeSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub model_ids: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceConfigSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub name: String,
    pub value: DataValue,
    pub category: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewayConfigSchema {
    pub id: i32,
    pub gateway_id: Uuid,
    pub name: String,
    pub value: DataValue,
    pub category: String
}

impl From<DeviceConfigSchema> for GatewayConfigSchema {
    fn from(value: DeviceConfigSchema) -> Self {
        Self {
            id: value.id,
            gateway_id: value.device_id,
            name: value.name,
            value: value.value,
            category: value.category
        }
    }
}

impl From<device::DeviceSchema> for DeviceSchema {
    fn from(value: device::DeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            gateway_id: Uuid::from_slice(&value.gateway_id).unwrap_or_default(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_: value.device_type.map(|s| s.into()).unwrap_or_default(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<device::DeviceSchema> for DeviceSchema {
    fn into(self) -> device::DeviceSchema {
        device::DeviceSchema {
            id: self.id.as_bytes().to_vec(),
            gateway_id: self.gateway_id.as_bytes().to_vec(),
            serial_number: self.serial_number,
            name: self.name,
            description: self.description,
            device_type: Some(self.type_.into()),
            configs: self.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<device::GatewaySchema> for GatewaySchema {
    fn from(value: device::GatewaySchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_:  value.gateway_type.map(|s| s.into()).unwrap_or_default(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<device::GatewaySchema> for GatewaySchema {
    fn into(self) -> device::GatewaySchema {
        device::GatewaySchema {
            id: self.id.as_bytes().to_vec(),
            serial_number: self.serial_number,
            name: self.name,
            description: self.description,
            gateway_type: Some(self.type_.into()),
            configs: self.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<device::ConfigSchema> for DeviceConfigSchema {
    fn from(value: device::ConfigSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes,
                DataType::from(value.config_type)
            ),
            category: value.category
        }
    }
}

impl Into<device::ConfigSchema> for DeviceConfigSchema {
    fn into(self) -> device::ConfigSchema {
        device::ConfigSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: self.value.get_type().into(),
            category: self.category
        }
    }
}

impl From<device::ConfigSchema> for GatewayConfigSchema {
    fn from(value: device::ConfigSchema) -> Self {
        Self {
            id: value.id,
            gateway_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes,
                DataType::from(value.config_type)
            ),
            category: value.category
        }
    }
}

impl Into<device::ConfigSchema> for GatewayConfigSchema {
    fn into(self) -> device::ConfigSchema {
        device::ConfigSchema {
            id: self.id,
            device_id: self.gateway_id.as_bytes().to_vec(),
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: self.value.get_type().into(),
            category: self.category
        }
    }
}

impl From<device::TypeSchema> for TypeSchema {
    fn from(value: device::TypeSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<device::TypeSchema> for TypeSchema {
    fn into(self) -> device::TypeSchema {
        device::TypeSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            model_ids: self.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct GroupSchema {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) description: String,
    pub(crate) members: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupModelSchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub model_ids: Vec<Uuid>
}

impl From<GroupSchema> for GroupModelSchema {
    fn from(value: GroupSchema) -> Self {
        Self {
            id: value.id,
            name: value.name,
            category: value.category,
            description: value.description,
            model_ids: value.members
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupDeviceSchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub device_ids: Vec<Uuid>
}

impl From<GroupSchema> for GroupDeviceSchema {
    fn from(value: GroupSchema) -> Self {
        Self {
            id: value.id,
            name: value.name,
            category: value.category,
            description: value.description,
            device_ids: value.members
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupGatewaySchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub gateway_ids: Vec<Uuid>
}

impl From<GroupSchema> for GroupGatewaySchema {
    fn from(value: GroupSchema) -> Self {
        Self {
            id: value.id,
            name: value.name,
            category: value.category,
            description: value.description,
            gateway_ids: value.members
        }
    }
}

impl From<group::GroupModelSchema> for GroupModelSchema {
    fn from(value: group::GroupModelSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupModelSchema> for GroupModelSchema {
    fn into(self) -> group::GroupModelSchema {
        group::GroupModelSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            model_ids: self.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

impl From<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn from(value: group::GroupDeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            device_ids: value.device_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn into(self) -> group::GroupDeviceSchema {
        group::GroupDeviceSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            device_ids: self.device_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

impl From<group::GroupDeviceSchema> for GroupGatewaySchema {
    fn from(value: group::GroupDeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            gateway_ids: value.device_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupDeviceSchema> for GroupGatewaySchema {
    fn into(self) -> group::GroupDeviceSchema {
        group::GroupDeviceSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            device_ids: self.gateway_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetSchema {
    pub id: Uuid,
    pub template_id: Uuid,
    pub name: String,
    pub description: String,
    pub members: Vec<SetMember>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetMember {
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub data_index: Vec<u8>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetTemplateSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub members: Vec<SetTemplateMember>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetTemplateMember {
    pub type_id: Uuid,
    pub model_id: Uuid,
    pub data_index: Vec<u8>
}

impl From<set::SetSchema> for SetSchema {
    fn from(value: set::SetSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            template_id: Uuid::from_slice(&value.template_id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<set::SetSchema> for SetSchema {
    fn into(self) -> set::SetSchema {
        set::SetSchema {
            id: self.id.as_bytes().to_vec(),
            template_id: self.template_id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            members: self.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<set::SetMember> for SetMember {
    fn from(value: set::SetMember) -> Self {
        Self {
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            data_index: value.data_index
        }
    }
}

impl Into<set::SetMember> for SetMember {
    fn into(self) -> set::SetMember {
        set::SetMember {
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            data_index: self.data_index
        }
    }
}

impl From<set::SetTemplateSchema> for SetTemplateSchema {
    fn from(value: set::SetTemplateSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<set::SetTemplateSchema> for SetTemplateSchema {
    fn into(self) -> set::SetTemplateSchema {
        set::SetTemplateSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            members: self.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<set::SetTemplateMember> for SetTemplateMember {
    fn from(value: set::SetTemplateMember) -> Self {
        Self {
            type_id: Uuid::from_slice(&value.type_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            data_index: value.data_index
        }
    }
}

impl Into<set::SetTemplateMember> for SetTemplateMember {
    fn into(self) -> set::SetTemplateMember {
        set::SetTemplateMember {
            type_id: self.type_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            data_index: self.data_index
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSchema {
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub tag: i16
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSetSchema {
    pub set_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub tag: i16
}

impl From<data::DataSchema> for DataSchema {
    fn from(value: data::DataSchema) -> Self {
        Self {
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            tag: value.tag as i16
        }
    }
}

impl Into<data::DataSchema> for DataSchema {
    fn into(self) -> data::DataSchema {
        data::DataSchema {
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: self.tag as i32
        }
    }
}

impl From<data::DataSetSchema> for DataSetSchema {
    fn from(value: data::DataSetSchema) -> Self {
        Self {
            set_id: Uuid::from_slice(&value.set_id).unwrap_or_default(),
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            tag: value.tag as i16
        }
    }
}

impl Into<data::DataSetSchema> for DataSetSchema {
    fn into(self) -> data::DataSetSchema {
        data::DataSetSchema {
            set_id: self.set_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: self.tag as i32
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct BufferSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub tag: i16
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct BufferSetSchema {
    pub ids: Vec<i32>,
    pub set_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub tag: i16
}

impl From<buffer::BufferSchema> for BufferSchema {
    fn from(value: buffer::BufferSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            tag: value.tag as i16
        }
    }
}

impl Into<buffer::BufferSchema> for BufferSchema {
    fn into(self) -> buffer::BufferSchema {
        buffer::BufferSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: self.tag as i32
        }
    }
}

impl From<buffer::BufferSetSchema> for BufferSetSchema {
    fn from(value: buffer::BufferSetSchema) -> Self {
        Self {
            ids: value.ids,
            set_id: Uuid::from_slice(&value.set_id).unwrap_or_default(),
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            tag: value.tag as i16
        }
    }
}

impl Into<buffer::BufferSetSchema> for BufferSetSchema {
    fn into(self) -> buffer::BufferSetSchema {
        buffer::BufferSetSchema {
            ids: self.ids,
            set_id: self.set_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: self.tag as i32
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SliceSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp_begin: DateTime<Utc>,
    pub timestamp_end: DateTime<Utc>,
    pub name: String,
    pub description: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SliceSetSchema {
    pub id: i32,
    pub set_id: Uuid,
    pub timestamp_begin: DateTime<Utc>,
    pub timestamp_end: DateTime<Utc>,
    pub name: String,
    pub description: String
}

impl From<slice::SliceSchema> for SliceSchema {
    fn from(value: slice::SliceSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp_begin: Utc.timestamp_nanos(value.timestamp_begin * 1000),
            timestamp_end: Utc.timestamp_nanos(value.timestamp_end * 1000),
            name: value.name,
            description: value.description
        }
    }
}

impl Into<slice::SliceSchema> for SliceSchema {
    fn into(self) -> slice::SliceSchema {
        slice::SliceSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp_begin: self.timestamp_begin.timestamp_micros(),
            timestamp_end: self.timestamp_end.timestamp_micros(),
            name: self.name,
            description: self.description
        }
    }
}

impl From<slice::SliceSetSchema> for SliceSetSchema {
    fn from(value: slice::SliceSetSchema) -> Self {
        Self {
            id: value.id,
            set_id: Uuid::from_slice(&value.set_id).unwrap_or_default(),
            timestamp_begin: Utc.timestamp_nanos(value.timestamp_begin * 1000),
            timestamp_end: Utc.timestamp_nanos(value.timestamp_end * 1000),
            name: value.name,
            description: value.description
        }
    }
}

impl Into<slice::SliceSetSchema> for SliceSetSchema {
    fn into(self) -> slice::SliceSetSchema {
        slice::SliceSetSchema {
            id: self.id,
            set_id: self.set_id.as_bytes().to_vec(),
            timestamp_begin: self.timestamp_begin.timestamp_micros(),
            timestamp_end: self.timestamp_end.timestamp_micros(),
            name: self.name,
            description: self.description
        }
    }
}

impl QueryStatement {

    pub(crate) async fn fetch_model_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ModelSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut last_tag: Option<i16> = None;
        let mut model_schema_vec: Vec<ModelSchemaFlat> = Vec::new();
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last model_schema in model_schema_vec or default
                let mut model_schema = model_schema_vec.pop().unwrap_or_default();
                // on every new id found insert model_schema to model_schema_vec and reset last_index
                let model_id: Uuid = row.get(0);
                if let Some(id) = last_id {
                    if id != model_id {
                        model_schema_vec.push(model_schema.clone());
                        model_schema = ModelSchemaFlat::default();
                        last_tag = None;
                    }
                }
                last_id = Some(model_id);
                model_schema.id = model_id;
                model_schema.name = row.get(1);
                model_schema.category = row.get(2);
                model_schema.description = row.get(3);
                model_schema.data_type = row.get::<Vec<u8>,_>(4).into_iter().map(|byte| byte.into()).collect();
                // on every new tag found, add a new tag schema to model schema and initialize a new config
                let tag_id = row.try_get(5).ok();
                let tag_name = row.try_get(6);
                let tag_bytes: Result<Vec<u8>,_> = row.try_get(7);
                if last_tag == None || last_tag != Some(tag_id.unwrap_or(0)) {
                    if let (Some(tag), Ok(name), Ok(bytes)) = 
                        (tag_id, tag_name, tag_bytes) 
                    {
                        let mut members = vec![tag];
                        for chunk in bytes.chunks_exact(2) {
                            members.push(i16::from_be_bytes([chunk[0], chunk[1]]));
                        }
                        model_schema.tags.push(TagSchema { model_id, tag, name, members });
                    }
                    model_schema.configs = Vec::new();
                }
                last_tag = Some(tag_id.unwrap_or(0));
                // update model_schema configs if non empty config found
                let config_id = row.try_get(8);
                let config_index = row.try_get(9);
                let config_name = row.try_get(10);
                let config_bytes: Result<Vec<u8>,_> = row.try_get(11);
                let config_type: Result<i16,_> = row.try_get(12);
                let config_category = row.try_get(13);
                if let (Ok(id), Ok(index), Ok(name), Ok(bytes), Ok(type_), Ok(category)) = 
                    (config_id, config_index, config_name, config_bytes, config_type, config_category) 
                {
                    let value = DataValue::from_bytes(&bytes, DataType::from(type_));
                    model_schema.configs.push(ModelConfigSchema { id, model_id, index, name, value, category});
                }
                // update model_schema_vec with updated model_schema
                model_schema_vec.push(model_schema.clone());
            })
            .fetch_all(pool)
            .await?;
    
        Ok(model_schema_vec.into_iter().map(|schema| schema.into()).collect())
    }

    pub(crate) async fn fetch_model_config_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ModelConfigSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes = row.get(4);
                let type_ = DataType::from(row.get::<i16,_>(5));
                ModelConfigSchema {
                    id: row.get(0),
                    model_id: row.get(1),
                    index: row.get(2),
                    name: row.get(3),
                    value: DataValue::from_bytes(bytes, type_),
                    category: row.get(6)
                }
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_tag_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TagSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let mut tags: Vec<i16> = vec![row.get(1)];
                let bytes: Vec<u8> = row.get(3);
                for chunk in bytes.chunks_exact(2) {
                    tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
                }
                TagSchema {
                    model_id: row.get(0),
                    tag: tags[0],
                    name: row.get(2),
                    members: tags
                }
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_tag_members(&self, pool: &Pool<Postgres>, tag: i16) -> Vec<i16>
    {
        let mut tags: Vec<i16> = vec![tag];
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes: Vec<u8> = row.get(0);
                for chunk in bytes.chunks_exact(2) {
                    tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
                }
            })
            .fetch_all(pool)
            .await
            .unwrap_or_default();
        tags.sort();
        tags.dedup();
        tags
    }

    pub(crate) async fn fetch_device_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DeviceSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut last_model: Option<Uuid> = None;
        let mut device_schema_vec: Vec<DeviceSchema> = Vec::new();
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last device_schema in device_schema_vec or default
                let mut device_schema = device_schema_vec.pop().unwrap_or_default();
                // on every new id found insert device_schema to device_schema_vec and reset last_model
                let device_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != device_id {
                        device_schema_vec.push(device_schema.clone());
                        device_schema = DeviceSchema::default();
                        last_model = None;
                    }
                }
                last_id = Some(device_id);
                device_schema.id = device_id;
                device_schema.gateway_id = row.get(1);
                device_schema.serial_number = row.get(3);
                device_schema.name = row.get(4);
                device_schema.description = row.get(5);
                device_schema.type_.id = row.get(2);
                device_schema.type_.name = row.get(6);
                device_schema.type_.description = row.get(7);
                // on every new model id found, add model id to type model and initialize a new config
                let model_id = row.try_get(8).ok();
                if last_model == None || last_model != Some(model_id.unwrap_or_default()) {
                    if let Some(id) = model_id {
                        device_schema.type_.model_ids.push(id);
                    }
                    device_schema.configs = Vec::new();
                }
                last_model = Some(model_id.unwrap_or_default());
                // update device_schema configs if non empty config found
                let config_id = row.try_get(9);
                let config_name = row.try_get(10);
                let config_bytes: Result<Vec<u8>,_> = row.try_get(11);
                let config_type: Result<i16,_> = row.try_get(12);
                let config_category = row.try_get(13);
                if let (Ok(id), Ok(name), Ok(bytes), Ok(type_), Ok(category)) = 
                    (config_id, config_name, config_bytes, config_type, config_category) 
                {
                    let value = DataValue::from_bytes(&bytes, DataType::from(type_));
                    device_schema.configs.push(DeviceConfigSchema { id, device_id, name, value, category });
                }
                // update device_schema_vec with updated device_schema
                device_schema_vec.push(device_schema.clone());
            })
            .fetch_all(pool)
            .await?;
    
        Ok(device_schema_vec)
    }

    pub(crate) async fn fetch_device_config_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DeviceConfigSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes = row.get(3);
                let type_ = DataType::from(row.get::<i16,_>(4));
                DeviceConfigSchema {
                    id: row.get(0),
                    device_id: row.get(1),
                    name: row.get(2),
                    value: DataValue::from_bytes(bytes, type_),
                    category: row.get(5)
                }
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_type_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TypeSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut type_schema_vec: Vec<TypeSchema> = Vec::new();
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last type_schema in type_schema_vec or default
                let mut type_schema = type_schema_vec.pop().unwrap_or_default();
                // on every new type_id found insert type_schema to type_schema_vec
                let type_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != type_id {
                        // insert new type_schema to type_schema_vec
                        type_schema_vec.push(type_schema.clone());
                        type_schema = TypeSchema::default();
                    }
                }
                last_id = Some(type_id);
                type_schema.id = type_id;
                type_schema.name = row.get(1);
                type_schema.description = row.get(2);
                // update type_schema if non empty model_id found
                let model_id: Result<Uuid, Error> = row.try_get(3);
                if let Ok(value) = model_id {
                    type_schema.model_ids.push(value);
                }
                // update type_schema_vec with updated type_schema
                type_schema_vec.push(type_schema.clone());
            })
            .fetch_all(pool)
            .await?;
    
        Ok(type_schema_vec)
    }

    pub(crate) async fn fetch_group_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<GroupSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut group_schema_vec: Vec<GroupSchema> = Vec::new();

        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last group_schema in group_schema_vec or default
                let mut group_schema = group_schema_vec.pop().unwrap_or_default();
                // on every new group_id found add id_vec and update group_schema scalar member
                let group_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != group_id {
                        // insert new type_schema to group_schema_vec
                        group_schema_vec.push(group_schema.clone());
                        group_schema = GroupSchema::default();
                    }
                }
                last_id = Some(group_id);
                group_schema.id = group_id;
                group_schema.name = row.get(1);
                group_schema.category = row.get(2);
                group_schema.description = row.get(3);
                // update group_schema if non empty member_id found
                let member_id: Result<Uuid, Error> = row.try_get(4);
                if let Ok(value) = member_id {
                    group_schema.members.push(value);
                }
                // update group_schema_vec with updated group_schema
                group_schema_vec.pop();
                group_schema_vec.push(group_schema.clone());
            })
            .fetch_all(pool)
            .await?;

        Ok(group_schema_vec)
    }

    pub(crate) async fn fetch_set_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SetSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut set_schema_vec: Vec<SetSchema> = Vec::new();
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last set_schema in set_schema_vec or default
                let mut set_schema = set_schema_vec.pop().unwrap_or_default();
                // on every new id found insert set_schema to set_schema_vec
                let id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != id {
                        set_schema_vec.push(set_schema.clone());
                        set_schema = SetSchema::default();
                    }
                }
                last_id = Some(id);
                set_schema.id = id;
                set_schema.template_id = row.get(1);
                set_schema.name = row.get(2);
                set_schema.description = row.get(3);
                // update set_schema members if non empty member found
                let id: Result<Uuid, Error> = row.try_get(4);
                if let Ok(device_id) = id {
                    set_schema.members.push(SetMember {
                        device_id,
                        model_id: row.try_get(5).unwrap_or_default(),
                        data_index: row.try_get(6).unwrap_or_default()
                    });
                }
                // update set_schema_vec with updated set_schema
                set_schema_vec.push(set_schema.clone());
            })
            .fetch_all(pool)
            .await?;
    
        Ok(set_schema_vec)
    }

    pub(crate) async fn fetch_set_members(&self, pool: &Pool<Postgres>) -> Result<Vec<SetMember>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
        .map(|row: PgRow| {
            SetMember {
                device_id: row.try_get(0).unwrap_or_default(),
                model_id: row.try_get(1).unwrap_or_default(),
                data_index: row.try_get(2).unwrap_or_default()
            }
        })
        .fetch_all(pool)
        .await
    }

    pub(crate) async fn fetch_set_template_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SetTemplateSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut template_schema_vec: Vec<SetTemplateSchema> = Vec::new();

        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last template_schema in template_schema_vec or default
                let mut template_schema = template_schema_vec.pop().unwrap_or_default();
                // on every new id found insert template_schema to template_schema_vec
                let id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != id {
                        template_schema_vec.push(template_schema.clone());
                        template_schema = SetTemplateSchema::default();
                    }
                }
                last_id = Some(id);
                template_schema.id = id;
                template_schema.name = row.get(1);
                template_schema.description = row.get(2);
                // update template_schema members if non empty member found
                let id: Result<Uuid, Error> = row.try_get(3);
                if let Ok(type_id) = id {
                    template_schema.members.push(SetTemplateMember {
                        type_id,
                        model_id: row.try_get(4).unwrap_or_default(),
                        data_index: row.try_get(5).unwrap_or_default()
                    });
                }
                // update template_schema_vec with updated template_schema
                template_schema_vec.push(template_schema.clone());
            })
            .fetch_all(pool)
            .await?;
    
        Ok(template_schema_vec)
    }

    pub(crate) async fn fetch_set_template_members(&self, pool: &Pool<Postgres>) -> Result<Vec<SetTemplateMember>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
        .map(|row: PgRow| {
            SetTemplateMember {
                type_id: row.try_get(0).unwrap_or_default(),
                model_id: row.try_get(1).unwrap_or_default(),
                data_index: row.try_get(2).unwrap_or_default()
            }
        })
        .fetch_all(pool)
        .await
    }

    pub(crate) async fn fetch_data_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DataSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes: Vec<u8> = row.get(4);
                let types: Vec<DataType> = row.get::<Vec<u8>,_>(5).into_iter().map(|ty| ty.into()).collect();
                DataSchema {
                    device_id: row.get(0),
                    model_id: row.get(1),
                    timestamp: row.get(2),
                    data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                    tag: row.get(3)
                }
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_data_types(&self, pool: &Pool<Postgres>) -> Result<Vec<Vec<DataType>>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_data_set_schema(&self, pool: &Pool<Postgres>, set_id: Uuid) -> Result<Vec<DataSetSchema>, Error>
    {
        let mut data_set_schema_vec: Vec<DataSetSchema> = Vec::new();
        let mut last_timestamp: Option<DateTime<Utc>> = None;
        let mut last_tag: Option<i16> = None;
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // construct a data_schema
                let bytes: Vec<u8> = row.get(4);
                let types: Vec<DataType> = row.get::<Vec<u8>,_>(5).into_iter().map(|ty| ty.into()).collect();
                let data_schema = DataSchema {
                    device_id: row.get(0),
                    model_id: row.get(1),
                    timestamp: row.get(2),
                    data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                    tag: row.get(3)
                };
                // get last data_set_schema in data_set_schema_vec
                let mut data_set_schema = data_set_schema_vec.pop().unwrap_or_default();
                // on every new timestamp or tag found, insert new data_set_schema to data_set_schema_vec
                if last_timestamp != Some(data_schema.timestamp) || last_tag != Some(data_schema.tag) {
                    if last_timestamp != None {
                        data_set_schema_vec.push(data_set_schema.clone());
                    }
                    // initialize data_set_schema data vector with Null
                    let number: i16 = row.get(8);
                    data_set_schema = DataSetSchema::default();
                    for _i in 0..number {
                        data_set_schema.data.push(DataValue::Null);
                    }
                }
                data_set_schema.set_id = set_id;
                data_set_schema.timestamp = data_schema.timestamp;
                data_set_schema.tag = data_schema.tag;
                let indexes: Vec<u8> = row.get(6);
                let position: i16 = row.get(7);
                // filter data vector by data_set data indexes of particular model
                // and replace data_set_schema data vector on the set position with filtered data vector
                for (position_offset, index) in indexes.into_iter().enumerate() {
                    data_set_schema.data[position as usize + position_offset] = 
                        data_schema.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
                }
                last_timestamp = Some(data_schema.timestamp);
                last_tag = Some(data_schema.tag);
                // update data_set_schema_vec with updated data_set_schema
                data_set_schema_vec.push(data_set_schema);
            })
            .fetch_all(pool)
            .await?;
    
        Ok(data_set_schema_vec)
    }

    pub(crate) async fn fetch_buffer_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<BufferSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(5);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(6).into_iter().map(|ty| ty.into()).collect();
            BufferSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                tag: row.get(4)
            }
        })
        .fetch_all(pool)
        .await
    }

    pub(crate) async fn fetch_buffer_types(&self, pool: &Pool<Postgres>) -> Result<Vec<DataType>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
            })
            .fetch_one(pool)
            .await
    }

    pub(crate) async fn fetch_buffer_set_schema(&self, pool: &Pool<Postgres>, set_id: Uuid) -> Result<Vec<BufferSetSchema>, Error>
    {
        let mut buffer_set_schema_vec: Vec<BufferSetSchema> = Vec::new();
        let mut last_timestamp: Option<DateTime<Utc>> = None;
        let mut last_tag: Option<i16> = None;
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // construct a buffer_schema
                let bytes: Vec<u8> = row.get(5);
                let types: Vec<DataType> = row.get::<Vec<u8>,_>(6).into_iter().map(|ty| ty.into()).collect();
                let buffer_schema = BufferSchema {
                    id: row.get(0),
                    device_id: row.get(1),
                    model_id: row.get(2),
                    timestamp: row.get(3),
                    data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                    tag: row.get(4)
                };
                // get last buffer_set_schema in buffer_set_schema_vec
                let mut buffer_set_schema = buffer_set_schema_vec.pop().unwrap_or_default();
                // on every new timestamp or tag found, insert new buffer_set_schema to buffer_set_schema_vec
                if last_timestamp != Some(buffer_schema.timestamp) || last_tag != Some(buffer_schema.tag) {
                    if last_timestamp != None {
                        buffer_set_schema_vec.push(buffer_set_schema.clone());
                    }
                    // initialize buffer_set_schema data vector with Null
                    let number: i16 = row.get(9);
                    buffer_set_schema = BufferSetSchema::default();
                    for _i in 0..number {
                        buffer_set_schema.data.push(DataValue::Null);
                    }
                }
                buffer_set_schema.ids.push(buffer_schema.id);
                buffer_set_schema.set_id = set_id;
                buffer_set_schema.timestamp = buffer_schema.timestamp;
                buffer_set_schema.tag = buffer_schema.tag;
                let indexes: Vec<u8> = row.get(7);
                let position: i16 = row.get(8);
                // filter data vector by data_set data indexes of particular model
                // and replace buffer_set_schema data vector on the set position with filtered data vector
                for (position_offset, index) in indexes.into_iter().enumerate() {
                    buffer_set_schema.data[position as usize + position_offset] = 
                    buffer_schema.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
                }
                last_timestamp = Some(buffer_schema.timestamp);
                last_tag = Some(buffer_schema.tag);
                // update buffer_set_schema_vec with updated buffer_set_schema
                buffer_set_schema_vec.push(buffer_set_schema);
            })
            .fetch_all(pool)
            .await?;
    
        Ok(buffer_set_schema_vec)
    }

    pub(crate) async fn fetch_slice_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SliceSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                SliceSchema {
                    id: row.get(0),
                    device_id: row.get(1),
                    model_id: row.get(2),
                    timestamp_begin: row.get(3),
                    timestamp_end: row.get(4),
                    name: row.get(5),
                    description: row.get(6)
                }
            })
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_slice_set_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SliceSetSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                SliceSetSchema {
                    id: row.get(0),
                    set_id: row.get(1),
                    timestamp_begin: row.get(2),
                    timestamp_end: row.get(3),
                    name: row.get(4),
                    description: row.get(5)
                }
            })
            .fetch_all(pool)
            .await
    }

}
