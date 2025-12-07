use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
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
