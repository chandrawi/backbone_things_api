use chrono::{Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::{DataType, DataValue, ArrayDataValue};
use bbthings_database::{
    ModelSchema, TagSchema, ModelConfigSchema, DeviceSchema, GatewaySchema,
    TypeSchema, TypeConfigSchema, DeviceConfigSchema, GatewayConfigSchema,
    GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema,
    SetSchema, SetMember, SetTemplateSchema, SetTemplateMember,
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema,
    SliceSchema, SliceSetSchema
};
use crate::proto::resource::{
    model, device, group, set, data, buffer, slice
};

impl From<ModelSchema> for model::ModelSchema {
    fn from(value: ModelSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            category: value.category,
            name: value.name,
            description: value.description,
            data_type: value.data_type.into_iter().map(|e| e.into()).collect(),
            tags: value.tags.into_iter().map(|t| t as i32).collect(),
            configs: value.configs.into_iter().map(|e| model::ConfigSchemaVec {
                    configs: e.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl From<model::ModelSchema> for ModelSchema {
    fn from(value: model::ModelSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            category: value.category,
            name: value.name,
            description: value.description,
            data_type: value.data_type.into_iter().map(|e| DataType::from(e)).collect(),
            tags: value.tags.into_iter().map(|t| t as i16).collect(),
            configs: value.configs.into_iter().map(|e| {
                    e.configs.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl From<TagSchema> for model::TagSchema {
    fn from(value: TagSchema) -> Self {
        Self {
            model_id: value.model_id.as_bytes().to_vec(),
            tag: value.tag as i32,
            name: value.name,
            members: value.members.into_iter().map(|v| v as i32).collect()
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

impl From<ModelConfigSchema> for model::ConfigSchema {
    fn from(value: ModelConfigSchema) -> Self {
        Self {
            id: value.id,
            model_id: value.model_id.as_bytes().to_vec(),
            index: value.index as i32,
            name: value.name,
            config_bytes: value.value.to_bytes(),
            config_type: value.value.get_type().into(),
            category: value.category
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

impl From<DeviceSchema> for device::DeviceSchema {
    fn from(value: DeviceSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            gateway_id: value.gateway_id.as_bytes().to_vec(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_id: value.type_id.as_bytes().to_vec(),
            type_name: value.type_name,
            model_ids: value.model_ids.into_iter().map(|id| id.as_bytes().to_vec()).collect(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
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
            type_id: Uuid::from_slice(&value.type_id).unwrap_or_default(),
            type_name: value.type_name,
            model_ids: value.model_ids.into_iter().map(|id| Uuid::from_slice(&id).unwrap_or_default()).collect(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<GatewaySchema> for device::GatewaySchema {
    fn from(value: GatewaySchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_id: value.type_id.as_bytes().to_vec(),
            type_name: value.type_name,
            model_ids: value.model_ids.into_iter().map(|v| v.as_bytes().to_vec()).collect(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
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
            type_id: Uuid::from_slice(&value.type_id).unwrap_or_default(),
            type_name: value.type_name,
            model_ids: value.model_ids.into_iter().map(|v| Uuid::from_slice(&v).unwrap_or_default()).collect(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<TypeSchema> for device::TypeSchema {
    fn from(value: TypeSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect(),
            configs: value.configs.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<device::TypeSchema> for TypeSchema {
    fn from(value: device::TypeSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect(),
            configs: value.configs.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<DeviceConfigSchema> for device::ConfigSchema {
    fn from(value: DeviceConfigSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.device_id.as_bytes().to_vec(),
            name: value.name,
            config_bytes: value.value.to_bytes(),
            config_type: value.value.get_type().into(),
            category: value.category
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

impl From<GatewayConfigSchema> for device::ConfigSchema {
    fn from(value: GatewayConfigSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.gateway_id.as_bytes().to_vec(),
            name: value.name,
            config_bytes: value.value.to_bytes(),
            config_type: value.value.get_type().into(),
            category: value.category
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

impl From<TypeConfigSchema> for device::TypeConfigSchema {
    fn from(value: TypeConfigSchema) -> Self {
        Self {
            id: value.id,
            type_id: value.type_id.as_bytes().to_vec(),
            name: value.name,
            config_type: value.value_type.into(),
            config_bytes: value.value_default.to_bytes(),
            category: value.category
        }
    }
}

impl From<device::TypeConfigSchema> for TypeConfigSchema {
    fn from(value: device::TypeConfigSchema) -> Self {
        Self {
            id: value.id,
            type_id: Uuid::from_slice(&value.type_id).unwrap_or_default(),
            name: value.name,
            value_type: DataType::from(value.config_type),
            value_default: DataValue::from_bytes(&value.config_bytes, DataType::from(value.config_type)),
            category: value.category
        }
    }
}

impl From<GroupModelSchema> for group::GroupModelSchema {
    fn from(value: GroupModelSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            category: value.category,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
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

impl From<GroupDeviceSchema> for group::GroupDeviceSchema {
    fn from(value: GroupDeviceSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            category: value.category,
            description: value.description,
            device_ids: value.device_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
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

impl From<GroupGatewaySchema> for group::GroupDeviceSchema {
    fn from(value: GroupGatewaySchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            category: value.category,
            description: value.description,
            device_ids: value.gateway_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
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

impl From<SetSchema> for set::SetSchema {
    fn from(value: SetSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            template_id: value.template_id.as_bytes().to_vec(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
        }
    }
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

impl From<SetMember> for set::SetMember {
    fn from(value: SetMember) -> Self {
        Self {
            device_id: value.device_id.as_bytes().to_vec(),
            model_id: value.model_id.as_bytes().to_vec(),
            data_index: value.data_index
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

impl From<SetTemplateSchema> for set::SetTemplateSchema {
    fn from(value: SetTemplateSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
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

impl From<SetTemplateMember> for set::SetTemplateMember {
    fn from(value: SetTemplateMember) -> Self {
        Self {
            type_id: value.type_id.as_bytes().to_vec(),
            model_id: value.model_id.as_bytes().to_vec(),
            data_index: value.data_index
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

impl From<DataSchema> for data::DataSchema {
    fn from(value: DataSchema) -> Self {
        Self {
            device_id: value.device_id.as_bytes().to_vec(),
            model_id: value.model_id.as_bytes().to_vec(),
            timestamp: value.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&value.data).to_bytes(),
            data_type: value.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: value.tag as i32
        }
    }
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

impl From<DataSetSchema> for data::DataSetSchema {
    fn from(value: DataSetSchema) -> Self {
        Self {
            set_id: value.set_id.as_bytes().to_vec(),
            timestamp: value.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&value.data).to_bytes(),
            data_type: value.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: value.tag as i32
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

impl From<BufferSchema> for buffer::BufferSchema {
    fn from(value: BufferSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.device_id.as_bytes().to_vec(),
            model_id: value.model_id.as_bytes().to_vec(),
            timestamp: value.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&value.data).to_bytes(),
            data_type: value.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: value.tag as i32
        }
    }
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

impl From<BufferSetSchema> for buffer::BufferSetSchema {
    fn from(value: BufferSetSchema) -> Self {
        Self {
            ids: value.ids,
            set_id: value.set_id.as_bytes().to_vec(),
            timestamp: value.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&value.data).to_bytes(),
            data_type: value.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: value.tag as i32
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

impl From<SliceSchema> for slice::SliceSchema {
    fn from(value: SliceSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.device_id.as_bytes().to_vec(),
            model_id: value.model_id.as_bytes().to_vec(),
            timestamp_begin: value.timestamp_begin.timestamp_micros(),
            timestamp_end: value.timestamp_end.timestamp_micros(),
            name: value.name,
            description: value.description
        }
    }
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

impl From<SliceSetSchema> for slice::SliceSetSchema {
    fn from(value: SliceSetSchema) -> Self {
        Self {
            id: value.id,
            set_id: value.set_id.as_bytes().to_vec(),
            timestamp_begin: value.timestamp_begin.timestamp_micros(),
            timestamp_end: value.timestamp_end.timestamp_micros(),
            name: value.name,
            description: value.description
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
