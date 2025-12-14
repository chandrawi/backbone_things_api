use sqlx::{Row, FromRow, Error, postgres::PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::resource::_schema::{
    ModelSchema, TagSchema, ModelConfigSchema, DeviceSchema, TypeSchema, DeviceConfigSchema, TypeConfigSchema,
    GroupSchema, SetSchema, SetMember, SetTemplateSchema, SetTemplateMember,
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema, SliceSchema, SliceSetSchema
};
use crate::common::type_value::{DataType, DataValue, ArrayDataValue};

pub(crate) struct ModelRow {
    model_id: Uuid,
    name: String,
    category: String,
    description: String,
    data_type: Vec<DataType>,
    tag: Option<i16>,
    tag_name: Option<String>,
    tag_members: Option<Vec<i16>>,
    config_id: Option<i32>,
    config_index: Option<i16>,
    config_name: Option<String>,
    config_value: Option<DataValue>,
    config_category: Option<String>
}

impl<'r> FromRow<'r, PgRow> for ModelRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let type_number_vec: Vec<u8> = row.try_get(4)?;
        let types: Vec<DataType> = type_number_vec.into_iter().map(|ty| ty.into()).collect();
        let bytes: Option<Vec<u8>> = row.try_get(11)?;
        let type_number: Option<i16> = row.try_get(12)?;
        let config_value = match (bytes, type_number) {
            (Some(b), Some(t)) => Some(DataValue::from_bytes(&b, DataType::from(t))),
            _ => None
        };
        Ok(Self {
            model_id: row.try_get(0)?,
            name: row.try_get(1)?,
            category: row.try_get(2)?,
            description: row.try_get(3)?,
            data_type: types,
            tag: row.try_get(5)?,
            tag_name: row.try_get(6)?,
            tag_members: row.try_get(7)?,
            config_id: row.try_get(8)?,
            config_index: row.try_get(9)?,
            config_name: row.try_get(10)?,
            config_value,
            config_category: row.try_get(13)?
        })
    }
}

pub(crate) fn map_to_model_schema(rows: Vec<ModelRow>) -> Vec<ModelSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // ModelRow is sorted by (model_id, tag, config_id) from query result

    let mut result = Vec::new();
    let mut last_model_id: Option<Uuid> = None;
    let mut last_model: Option<ModelSchema> = None;

    for row in rows {
        // 1) Detect new model row
        if Some(row.model_id) != last_model_id {
            // Push previous model
            if let Some(model) = last_model.take() {
                result.push(model);
            }
            // start new model
            // initialize config with vector with the same length as data_type
            last_model_id = Some(row.model_id);
            let length = row.data_type.len();
            last_model = Some(ModelSchema {
                id: row.model_id,
                name: row.name,
                category: row.category,
                description: row.description,
                data_type: row.data_type,
                tags: Vec::new(),
                configs: (0..length).map(|_| Vec::new()).collect()
            });
        }

        if let Some(model) = last_model.as_mut() {
            // 2) Add tag if exists without duplicates
            if let Some(tag) = row.tag {
                if model.tags.last().map(|t| t.tag) != Some(tag) {
                    model.tags.push(TagSchema {
                        model_id: row.model_id,
                        tag,
                        name: row.tag_name.unwrap_or_default(),
                        members: row.tag_members.unwrap_or(vec![tag])
                    });
                }
            }

            // 3) Add config if exists
            if let (Some(config_id), Some(index)) = (row.config_id, row.config_index) {
                // check if the last model has config vector with its length greater than the config index
                if let Some(configs) = model.configs.get_mut(index as usize) {
                    let exists = configs.iter().any(|c| c.id == config_id);
                    if !exists {
                        // push previous config to last_model
                        configs.push(ModelConfigSchema {
                            id: config_id,
                            model_id: row.model_id,
                            index,
                            name: row.config_name.unwrap_or_default(),
                            value: row.config_value.unwrap_or_default(),
                            category: row.config_category.unwrap_or_default()
                        });
                    }
                }
            }
        }
    }

    // Push last model
    if let Some(model) = last_model.take() {
        result.push(model);
    }
    result.into_iter().map(|s| s.into()).collect()
}

impl<'r> FromRow<'r, PgRow> for TagSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let mut tags: Vec<i16> = vec![row.try_get(1)?];
        let bytes: Vec<u8> = row.try_get(3)?;
        for chunk in bytes.chunks_exact(2) {
            tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
        }
        Ok(Self {
            model_id: row.try_get(0)?,
            tag: tags[0],
            name: row.try_get(2)?,
            members: tags
        })
    }
}

impl<'r> FromRow<'r, PgRow> for ModelConfigSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes = row.try_get(4)?;
        let type_number: i16 = row.try_get(5)?;
        Ok(Self {
            id: row.try_get(0)?,
            model_id: row.try_get(1)?,
            index: row.try_get(2)?,
            name: row.try_get(3)?,
            value: DataValue::from_bytes(bytes, DataType::from(type_number)),
            category: row.try_get(6)?
        })
    }
}

pub(crate) struct DeviceRow {
    device_id: Uuid,
    gateway_id: Uuid,
    type_id: Uuid,
    serial_number: String,
    name: String,
    description: String,
    type_name: String,
    model_id: Option<Uuid>,
    config_id: Option<i32>,
    config_name: Option<String>,
    config_value: Option<DataValue>,
    config_category: Option<String>
}

impl<'r> FromRow<'r, PgRow> for DeviceRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Option<Vec<u8>> = row.try_get(10)?;
        let type_number: Option<i16> = row.try_get(11)?;
        let config_value = match (bytes, type_number) {
            (Some(b), Some(t)) => Some(DataValue::from_bytes(&b, DataType::from(t))),
            _ => None
        };
        Ok(Self {
            device_id: row.try_get(0)?,
            gateway_id: row.try_get(1)?,
            type_id: row.try_get(2)?,
            serial_number: row.try_get(3)?,
            name: row.try_get(4)?,
            description: row.try_get(5)?,
            type_name: row.try_get(6)?,
            model_id: row.try_get(7)?,
            config_id: row.try_get(8)?,
            config_name: row.try_get(9)?,
            config_value,
            config_category: row.try_get(12)?
        })
    }
}

pub(crate) fn map_to_device_schema(rows: Vec<DeviceRow>) -> Vec<DeviceSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // DeviceRow is sorted by (device_id, model_id, config_id) from query result

    let mut result = Vec::new();
    let mut last_device_id: Option<Uuid> = None;
    let mut last_device: Option<DeviceSchema> = None;

    for row in rows {
        // 1) Detect new device row
        if Some(row.device_id) != last_device_id {
            // Push previous device
            if let Some(device) = last_device.take() {
                result.push(device);
            }
            // start new device
            last_device_id = Some(row.device_id);
            last_device = Some(DeviceSchema {
                id: row.device_id,
                gateway_id: row.gateway_id,
                serial_number: row.serial_number,
                name: row.name,
                description: row.description,
                type_id: row.type_id,
                type_name: row.type_name,
                model_ids: Vec::new(),
                configs: Vec::new()
            });
        }

        if let Some(device) = last_device.as_mut() {
            // 2) Add model_id if exists without duplicates
            if let Some(model_id) = row.model_id {
                if device.model_ids.last() !=  Some(&model_id) {
                    device.model_ids.push(model_id);
                }
            }
    
            // 3) Add device config if exists without duplicates
            if let Some(config_id) = row.config_id {
                let exists = device.configs.iter().any(|c| c.id == config_id);
                if !exists {
                    // push previous config to last_device
                    device.configs.push(DeviceConfigSchema {
                        id: config_id,
                        device_id: row.device_id,
                        name: row.config_name.unwrap_or_default(),
                        value: row.config_value.unwrap_or_default(),
                        category: row.config_category.unwrap_or_default()
                    });
                }
            }
        }
    }

    // Push last device and last type
    if let Some(device) = last_device.take() {
        result.push(device);
    }
    result
}

pub(crate) struct TypeRow {
    type_id: Uuid,
    name: String,
    description: String,
    model_id: Option<Uuid>,
    config_id: Option<i32>,
    config_name: Option<String>,
    config_type: Option<DataType>,
    config_category: Option<String>
}

impl<'r> FromRow<'r, PgRow> for TypeRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let type_number: Option<i16> = row.try_get(6)?;
        Ok(Self {
            type_id: row.try_get(0)?,
            name: row.try_get(1)?,
            description: row.try_get(2)?,
            model_id: row.try_get(3)?,
            config_id: row.try_get(4)?,
            config_name: row.try_get(5)?,
            config_type: type_number.map(|t| DataType::from(t)),
            config_category: row.try_get(7)?
        })
    }
}

pub(crate) fn map_to_type_schema(rows: Vec<TypeRow>) -> Vec<TypeSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // TypeRow is sorted by (type_id, model_id) from query result

    let mut result = Vec::new();
    let mut last_type_id: Option<Uuid> = None;
    let mut last_type: Option<TypeSchema> = None;

    for row in rows {
        // 1) Detect new type row
        if Some(row.type_id) != last_type_id {
            // Push previous type
            if let Some(type_) = last_type.take() {
                result.push(type_);
            }
            last_type_id = Some(row.type_id);
            last_type = Some(TypeSchema {
                id: row.type_id,
                name: row.name,
                description: row.description,
                model_ids: Vec::new(),
                configs: Vec::new()
            });
        }

        if let Some(type_) = last_type.as_mut() {
            // 2) Add model_id if exists without duplicates
            if let Some(model_id) = row.model_id {
                if type_.model_ids.last() !=  Some(&model_id) {
                    type_.model_ids.push(model_id);
                }
            }

            // 3) Add type config if exists without duplicates
            if let Some(config_id) = row.config_id {
                let exists = type_.configs.iter().any(|c| c.id == config_id);
                if !exists {
                    // push previous config to last_device
                    type_.configs.push(TypeConfigSchema {
                        id: config_id,
                        type_id: row.type_id,
                        name: row.config_name.unwrap_or_default(),
                        value_type: row.config_type.unwrap_or_default(),
                        category: row.config_category.unwrap_or_default()
                    });
                }
            }
        }
    }

    // Push last type
    if let Some(type_) = last_type.take() {
        result.push(type_);
    }
    result
}

impl<'r> FromRow<'r, PgRow> for DeviceConfigSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes = row.try_get(3)?;
        let type_number: i16 = row.try_get(4)?;
        Ok(Self {
            id: row.try_get(0)?,
            device_id: row.try_get(1)?,
            name: row.try_get(2)?,
            value: DataValue::from_bytes(bytes, DataType::from(type_number)),
            category: row.try_get(5)?
        })
    }
}

impl<'r> FromRow<'r, PgRow> for TypeConfigSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let type_number: i16 = row.try_get(3)?;
        Ok(Self {
            id: row.try_get(0)?,
            type_id: row.try_get(1)?,
            name: row.try_get(2)?,
            value_type: DataType::from(type_number),
            category: row.try_get(4)?
        })
    }
}

pub(crate) struct GroupRow {
    group_id: Uuid,
    name: String,
    category: String,
    description: String,
    member_id: Option<Uuid>
}

impl<'r> FromRow<'r, PgRow> for GroupRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            group_id: row.try_get(0)?,
            name: row.try_get(1)?,
            category: row.try_get(2)?,
            description: row.try_get(3)?,
            member_id: row.try_get(4)?
        })
    }
}

pub(crate) fn map_to_group_schema(rows: Vec<GroupRow>) -> Vec<GroupSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // GroupRow is sorted by (group_id, device_id or model_id) from query result

    let mut result = Vec::new();
    let mut last_group_id: Option<Uuid> = None;
    let mut last_group: Option<GroupSchema> = None;

    for row in rows {
        // 1) Detect new group row
        if Some(row.group_id) != last_group_id {
            // Push previous group
            if let Some(group) = last_group.take() {
                result.push(group);
            }
            last_group_id = Some(row.group_id);
            last_group = Some(GroupSchema {
                id: row.group_id,
                name: row.name,
                category: row.category,
                description: row.description,
                members: Vec::new(),
            });
        }

        // 2) Add member_id if exists
        if let (Some(group), Some(member_id)) = (last_group.as_mut(), row.member_id) {
            group.members.push(member_id);
        }
    }

    // Push last group
    if let Some(group) = last_group.take() {
        result.push(group);
    }
    result
}

pub(crate) struct SetRow {
    set_id: Uuid,
    template_id: Uuid,
    name: String,
    description: String,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    data_index: Option<Vec<u8>>
}

impl<'r> FromRow<'r, PgRow> for SetRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            set_id: row.try_get(0)?,
            template_id: row.try_get(1)?,
            name: row.try_get(2)?,
            description: row.try_get(3)?,
            device_id: row.try_get(4)?,
            model_id: row.try_get(5)?,
            data_index: row.try_get(6)?
        })
    }
}

pub(crate) fn map_to_set_schema(rows: Vec<SetRow>) -> Vec<SetSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // SetRow is sorted by (set_id, set_position) from query result

    let mut result = Vec::new();
    let mut last_set_id: Option<Uuid> = None;
    let mut last_set: Option<SetSchema> = None;

    for row in rows {
        // 1) Detect new set row
        if Some(row.set_id) != last_set_id {
            // Push previous set
            if let Some(set) = last_set.take() {
                result.push(set);
            }
            last_set_id = Some(row.set_id);
            last_set = Some(SetSchema {
                id: row.set_id,
                template_id: row.template_id,
                name: row.name,
                description: row.description,
                members: Vec::new()
            });
        }

        // 2) Add new member if exists
        if let (Some(set), Some(device_id), Some(model_id)) = (last_set.as_mut(), row.device_id, row.model_id) {
            set.members.push(SetMember {
                device_id,
                model_id,
                data_index: row.data_index.unwrap_or_default()
            });
        }
    }

    // Push last set
    if let Some(set) = last_set.take() {
        result.push(set);
    }
    result
}

impl<'r> FromRow<'r, PgRow> for SetMember {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            device_id: row.try_get(0)?,
            model_id: row.try_get(1)?,
            data_index: row.try_get(2)?
        })
    }
}

pub(crate) struct SetTemplateRow {
    template_id: Uuid,
    name: String,
    description: String,
    type_id: Option<Uuid>,
    model_id: Option<Uuid>,
    data_index: Option<Vec<u8>>
}

impl<'r> FromRow<'r, PgRow> for SetTemplateRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            template_id: row.try_get(0)?,
            name: row.try_get(1)?,
            description: row.try_get(2)?,
            type_id: row.try_get(3)?,
            model_id: row.try_get(4)?,
            data_index: row.try_get(5)?
        })
    }
}

pub(crate) fn map_to_set_template_schema(rows: Vec<SetTemplateRow>) -> Vec<SetTemplateSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // SetTemplateRow is sorted by (template_id, template_index) from query result

    let mut result = Vec::new();
    let mut last_template_id: Option<Uuid> = None;
    let mut last_template: Option<SetTemplateSchema> = None;

    for row in rows {
        // 1) Detect new template row
        if Some(row.template_id) != last_template_id {
            // Push previous template
            if let Some(template) = last_template.take() {
                result.push(template);
            }
            last_template_id = Some(row.template_id);
            last_template = Some(SetTemplateSchema {
                id: row.template_id,
                name: row.name,
                description: row.description,
                members: Vec::new()
            });
        }

        // 2) Add new member if exists
        if let (Some(template), Some(type_id), Some(model_id)) = (last_template.as_mut(), row.type_id, row.model_id) {
            template.members.push(SetTemplateMember {
                type_id,
                model_id,
                data_index: row.data_index.unwrap_or_default()
            });
        }
    }

    // Push last set template
    if let Some(template) = last_template.take() {
        result.push(template);
    }
    result
}

impl<'r> FromRow<'r, PgRow> for SetTemplateMember {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            type_id: row.try_get(0)?,
            model_id: row.try_get(1)?,
            data_index: row.try_get(2)?
        })
    }
}

impl<'r> FromRow<'r, PgRow> for DataSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Vec<u8> = row.try_get(4)?;
        let type_number_vec: Vec<u8> = row.try_get(5)?;
        let types: Vec<DataType> = type_number_vec.into_iter().map(|ty| ty.into()).collect();
        Ok(Self {
            device_id: row.try_get(0)?,
            model_id: row.try_get(1)?,
            timestamp: row.try_get(2)?,
            data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
            tag: row.try_get(3)?
        })
    }
}

pub(crate) struct DataSetRow {
    _device_id: Uuid,
    _model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>,
    tag: i16,
    data_index: Vec<u8>,
    set_position: i16,
    set_number: i16
}

impl<'r> FromRow<'r, PgRow> for DataSetRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Vec<u8> = row.try_get(4)?;
        let type_number_vec: Vec<u8> = row.try_get(5)?;
        let types: Vec<DataType> = type_number_vec.into_iter().map(|ty| ty.into()).collect();
        Ok(Self {
            _device_id: row.try_get(0)?,
            _model_id: row.try_get(1)?,
            timestamp: row.try_get(2)?,
            data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
            tag: row.try_get(3)?,
            data_index: row.try_get(6)?,
            set_position: row.try_get(7)?,
            set_number: row.try_get(8)?
        })
    }
}

pub(crate) fn map_to_dataset_schema(rows: Vec<DataSetRow>, set_id: Uuid) -> Vec<DataSetSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // DataSetRow is sorted by (timestamp, tag, set_position) from query result

    let mut result = Vec::new();
    let mut last_timestamp: Option<DateTime<Utc>> = None;
    let mut last_tag: Option<i16> = None;
    let mut last_dataset: DataSetSchema = DataSetSchema::default();

    for row in rows {
        // 1) Detect new timestamp or tag
        if Some(row.timestamp) != last_timestamp || Some(row.tag) != last_tag {
            // push old dataset schema
            if last_timestamp != None && last_tag != None {
                result.push(last_dataset);
            }
            // reset current dataset schema and initialize data vector with vector of Null with set_number length
            last_dataset = DataSetSchema {
                set_id,
                timestamp: row.timestamp,
                data: (0..row.set_number).map(|_| DataValue::Null).collect(),
                tag: row.tag
            };
            // set current timestamp and tag value
            last_timestamp = Some(row.timestamp);
            last_tag= Some(row.tag);
        }
        // 2) replace current dataset schema data vector at (set_position + offset) index with dataset row data at data_index
        for (position_offset, index) in row.data_index.into_iter().enumerate() {
            if let Some(data) = last_dataset.data.get_mut(row.set_position as usize + position_offset) {
                *data = row.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
            }
        }
    }

    // Push last dataset schema
    result.push(last_dataset);
    result
}

impl<'r> FromRow<'r, PgRow> for BufferSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Vec<u8> = row.try_get(5)?;
        let type_number_vec: Vec<u8> = row.try_get(6)?;
        let types: Vec<DataType> = type_number_vec.into_iter().map(|ty| ty.into()).collect();
        Ok(Self {
            id: row.try_get(0)?,
            device_id: row.try_get(1)?,
            model_id: row.try_get(2)?,
            timestamp: row.try_get(3)?,
            data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
            tag: row.try_get(4)?
        })
    }
}

pub(crate) struct BufferSetRow {
    id: i32,
    _device_id: Uuid,
    _model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>,
    tag: i16,
    data_index: Vec<u8>,
    set_position: i16,
    set_number: i16
}

impl<'r> FromRow<'r, PgRow> for BufferSetRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Vec<u8> = row.try_get(5)?;
        let type_number_vec: Vec<u8> = row.try_get(6)?;
        let types: Vec<DataType> = type_number_vec.into_iter().map(|ty| ty.into()).collect();
        Ok(Self {
            id: row.try_get(0)?,
            _device_id: row.try_get(1)?,
            _model_id: row.try_get(2)?,
            timestamp: row.try_get(3)?,
            data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
            tag: row.try_get(4)?,
            data_index: row.try_get(7)?,
            set_position: row.try_get(8)?,
            set_number: row.try_get(9)?
        })
    }
}

pub(crate) fn map_to_bufferset_schema(rows: Vec<BufferSetRow>, set_id: Uuid) -> Vec<BufferSetSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // BufferSetRow is sorted by (timestamp, tag, set_position) from query result

    let mut result = Vec::new();
    let mut last_timestamp: Option<DateTime<Utc>> = None;
    let mut last_tag: Option<i16> = None;
    let mut last_bufferset: BufferSetSchema = BufferSetSchema::default();

    for row in rows {
        // 1) Detect new timestamp or tag
        if Some(row.timestamp) != last_timestamp || Some(row.tag) != last_tag {
            // push old bufferset schema
            if last_timestamp != None && last_tag != None {
                result.push(last_bufferset);
            }
            // reset current bufferset schema and initialize data vector with vector of Null with set_number length
            last_bufferset = BufferSetSchema {
                ids: Vec::new(),
                set_id,
                timestamp: row.timestamp,
                data: (0..row.set_number).map(|_| DataValue::Null).collect(),
                tag: row.tag
            };
            // set current timestamp and tag value
            last_timestamp = Some(row.timestamp);
            last_tag= Some(row.tag);
        }
        // 2) replace current bufferset schema data vector at (set_position + offset) index with bufferset row data at data_index
        for (position_offset, index) in row.data_index.into_iter().enumerate() {
            if let Some(data) = last_bufferset.data.get_mut(row.set_position as usize + position_offset) {
                *data = row.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
            }
        }
        // 3) add bufferset row id to bufferset schema ids
        last_bufferset.ids.push(row.id);
    }

    // Push last bufferset schema
    result.push(last_bufferset);
    result
}

impl<'r> FromRow<'r, PgRow> for SliceSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            id: row.try_get(0)?,
            device_id: row.try_get(1)?,
            model_id: row.try_get(2)?,
            timestamp_begin: row.try_get(3)?,
            timestamp_end: row.try_get(4)?,
            name: row.try_get(5)?,
            description: row.try_get(6)?
        })
    }
}

impl<'r> FromRow<'r, PgRow> for SliceSetSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            id: row.try_get(0)?,
            set_id: row.try_get(1)?,
            timestamp_begin: row.try_get(2)?,
            timestamp_end: row.try_get(3)?,
            name: row.try_get(4)?,
            description: row.try_get(5)?
        })
    }
}
