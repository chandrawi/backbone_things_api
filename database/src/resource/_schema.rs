use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::type_value::{DataType, DataValue};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelSchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub data_type: Vec<DataType>,
    pub tags: Vec<i16>,
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

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceSchema {
    pub id: Uuid,
    pub gateway_id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_id: Uuid,
    pub type_name: String,
    pub model_ids: Vec<Uuid>,
    pub configs: Vec<DeviceConfigSchema>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewaySchema {
    pub id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_id: Uuid,
    pub type_name: String,
    pub model_ids: Vec<Uuid>,
    pub configs: Vec<GatewayConfigSchema>
}

impl From<DeviceSchema> for GatewaySchema {
    fn from(value: DeviceSchema) -> Self {
        Self {
            id: value.gateway_id,
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_id: value.type_id,
            type_name: value.type_name,
            model_ids: value.model_ids,
            configs: value.configs.into_iter().map(|el| el.into()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TypeSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub model_ids: Vec<Uuid>,
    pub configs: Vec<TypeConfigSchema>
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

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TypeConfigSchema {
    pub id: i32,
    pub type_id: Uuid,
    pub name: String,
    pub value_type: DataType,
    pub value_default: DataValue,
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

pub(crate) struct SetMemberSort {
    pub(crate) device_id: Uuid,
    pub(crate) model_id: Uuid,
    pub(crate) data_index: Vec<u8>,
    pub(crate) set_position: i16
}

impl From<SetMemberSort> for SetMember {
    fn from(value: SetMemberSort) -> Self {
        Self {
            device_id: value.device_id,
            model_id: value.model_id,
            data_index: value.data_index
        }
    }
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

pub(crate) struct SetTemplateMemberSort {
    pub(crate) type_id: Uuid,
    pub(crate) model_id: Uuid,
    pub(crate) data_index: Vec<u8>,
    pub(crate) template_index: i16
}

impl From<SetTemplateMemberSort> for SetTemplateMember {
    fn from(value: SetTemplateMemberSort) -> Self {
        Self {
            type_id: value.type_id,
            model_id: value.model_id,
            data_index: value.data_index
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
