use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::common::type_value::{DataType, DataValue};

#[derive(Iden)]
pub(crate) enum Device {
    Table,
    DeviceId,
    GatewayId,
    TypeId,
    SerialNumber,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceType {
    Table,
    TypeId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceTypeModel {
    Table,
    TypeId,
    ModelId
}

#[derive(Iden)]
pub(crate) enum DeviceConfig {
    Table,
    Id,
    DeviceId,
    Name,
    Category,
    Type,
    Value
}

#[derive(Iden)]
pub(crate) enum DeviceTypeConfig {
    Table,
    Id,
    TypeId,
    Name,
    Category,
    Type,
    Value,
}

pub enum DeviceKind {
    Device,
    Gateway
}

pub fn select_device(
    kind: DeviceKind,
    id: Option<Uuid>,
    serial_number: Option<&str>,
    ids: Option<&[Uuid]>,
    gateway_id: Option<Uuid>,
    type_id: Option<Uuid>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (Device::Table, Device::DeviceId),
            (Device::Table, Device::GatewayId),
            (Device::Table, Device::TypeId),
            (Device::Table, Device::SerialNumber),
            (Device::Table, Device::Name),
            (Device::Table, Device::Description)
        ])
        .columns([
            (DeviceType::Table, DeviceType::Name)
        ])
        .columns([
            (DeviceTypeModel::Table, DeviceTypeModel::ModelId)
        ])
        .columns([
            (DeviceConfig::Table, DeviceConfig::Id),
            (DeviceConfig::Table, DeviceConfig::Name),
            (DeviceConfig::Table, DeviceConfig::Category),
            (DeviceConfig::Table, DeviceConfig::Type),
            (DeviceConfig::Table, DeviceConfig::Value)
        ])
        .from(Device::Table)
        .inner_join(DeviceType::Table, 
            Expr::col((Device::Table, Device::TypeId))
            .equals((DeviceType::Table, DeviceType::TypeId))
        )
        .left_join(DeviceTypeModel::Table, 
            Expr::col((Device::Table, Device::TypeId))
            .equals((DeviceTypeModel::Table, DeviceTypeModel::TypeId))
        )
        .left_join(DeviceConfig::Table, 
            Expr::col((Device::Table, Device::DeviceId))
            .equals((DeviceConfig::Table, DeviceConfig::DeviceId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::DeviceId)).eq(id)).to_owned();
    }
    else if let Some(sn) = serial_number {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::SerialNumber)).eq(sn.to_owned())).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::DeviceId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(gateway_id) = gateway_id {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::GatewayId)).eq(gateway_id)).to_owned();
        }
        if let Some(type_id) = type_id {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::TypeId)).eq(type_id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Device::Table, Device::Name)).like(name_like)).to_owned();
        }
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((Device::Table, Device::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let stmt = stmt
        .order_by((Device::Table, Device::DeviceId), Order::Asc)
        .order_by((DeviceTypeModel::Table, DeviceTypeModel::ModelId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_device(
    id: Uuid,
    gateway_id: Uuid,
    type_id: Uuid,
    serial_number: &str,
    name: &str,
    description: Option<&str>
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(Device::Table)
        .columns([
            Device::DeviceId,
            Device::GatewayId,
            Device::TypeId,
            Device::SerialNumber,
            Device::Name,
            Device::Description
        ])
        .values([
            id.into(),
            gateway_id.into(),
            type_id.into(),
            serial_number.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_device(
    kind: DeviceKind,
    id: Uuid,
    gateway_id: Option<Uuid>,
    type_id: Option<Uuid>,
    serial_number: Option<&str>,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(Device::Table)
        .to_owned();

    if let Some(value) = gateway_id {
        stmt = stmt.value(Device::GatewayId, value).to_owned();
    }
    if let Some(value) = type_id {
        stmt = stmt.value(Device::TypeId, value).to_owned();
    }
    if let Some(value) = serial_number {
        stmt = stmt.value(Device::SerialNumber, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Device::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Device::Description, value).to_owned();
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(Expr::col(Device::GatewayId).eq(id)).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(Device::DeviceId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_device(
    kind: DeviceKind,
    id: Uuid
) -> QueryStatement
{
    let mut stmt = Query::delete()
        .from_table(Device::Table)
        .and_where(Expr::col(Device::DeviceId).eq(id))
        .to_owned();

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(Expr::col(Device::GatewayId).eq(id)).to_owned();
    }
    let stmt = stmt.to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_device_config(
    kind: DeviceKind,
    id: Option<i32>,
    device_id: Option<Uuid>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (DeviceConfig::Table, DeviceConfig::Id),
            (DeviceConfig::Table, DeviceConfig::DeviceId),
            (DeviceConfig::Table, DeviceConfig::Name),
            (DeviceConfig::Table, DeviceConfig::Category),
            (DeviceConfig::Table, DeviceConfig::Type),
            (DeviceConfig::Table, DeviceConfig::Value)
        ])
        .columns([
            (Device::Table, Device::GatewayId)
        ])
        .from(DeviceConfig::Table)
        .inner_join(Device::Table, 
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId))
            .equals((Device::Table, Device::DeviceId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::Id)).eq(id)).to_owned();
    }
    else if let Some(device_id) = device_id {
        stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).eq(device_id)).to_owned();
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let stmt = stmt
        .order_by((DeviceConfig::Table, DeviceConfig::DeviceId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_device_config(
    device_id: Uuid,
    name: &str,
    value: DataValue,
    category: &str
) -> QueryStatement
{
    let config_value = value.to_bytes();
    let config_type = i16::from(value.get_type());
    let stmt = Query::insert()
        .into_table(DeviceConfig::Table)
        .columns([
            DeviceConfig::DeviceId,
            DeviceConfig::Name,
            DeviceConfig::Value,
            DeviceConfig::Type,
            DeviceConfig::Category
        ])
        .values([
            device_id.into(),
            name.into(),
            config_value.into(),
            config_type.into(),
            category.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(DeviceConfig::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_device_config(
    id: i32,
    name: Option<&str>,
    value: Option<DataValue>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(DeviceConfig::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(DeviceConfig::Name, value).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(DeviceConfig::Value, bytes)
            .value(DeviceConfig::Type, type_).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(DeviceConfig::Category, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_device_config(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(DeviceConfig::Table)
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_device_type(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (DeviceType::Table, DeviceType::TypeId),
            (DeviceType::Table, DeviceType::Name),
            (DeviceType::Table, DeviceType::Description)
        ])
        .columns([
            (DeviceTypeModel::Table, DeviceTypeModel::ModelId)
        ])
        .columns([
            (DeviceTypeConfig::Table, DeviceTypeConfig::Id),
            (DeviceTypeConfig::Table, DeviceTypeConfig::Name),
            (DeviceTypeConfig::Table, DeviceTypeConfig::Category),
            (DeviceTypeConfig::Table, DeviceTypeConfig::Type),
            (DeviceTypeConfig::Table, DeviceTypeConfig::Value)
        ])
        .from(DeviceType::Table)
        .left_join(DeviceTypeModel::Table, 
            Expr::col((DeviceType::Table, DeviceType::TypeId))
            .equals((DeviceTypeModel::Table, DeviceTypeModel::TypeId))
        )
        .left_join(DeviceTypeConfig::Table, 
            Expr::col((DeviceType::Table, DeviceType::TypeId))
            .equals((DeviceTypeConfig::Table, DeviceTypeConfig::TypeId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::TypeId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::TypeId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::Name)).like(name_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by((DeviceType::Table, DeviceType::TypeId), Order::Asc)
        .order_by((DeviceTypeModel::Table, DeviceTypeModel::ModelId), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_device_type(
    id: Uuid,
    name: &str,
    description: Option<&str>
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(DeviceType::Table)
        .columns([
            DeviceType::TypeId,
            DeviceType::Name,
            DeviceType::Description
        ])
        .values([
            id.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_device_type(
    id: Uuid,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(DeviceType::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(DeviceType::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(DeviceType::Description, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(DeviceType::TypeId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_device_type(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(DeviceType::Table)
        .and_where(Expr::col(DeviceType::TypeId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn insert_device_type_model(
    id: Uuid,
    model_id: Uuid
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(DeviceTypeModel::Table)
        .columns([
            DeviceTypeModel::TypeId,
            DeviceTypeModel::ModelId
        ])
        .values([
            id.into(),
            model_id.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn delete_device_type_model(
    id: Uuid,
    model_id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(DeviceTypeModel::Table)
        .and_where(Expr::col(DeviceTypeModel::TypeId).eq(id))
        .and_where(Expr::col(DeviceTypeModel::ModelId).eq(model_id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_device_type_config(
    id: Option<i32>,
    type_id: Option<Uuid>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            DeviceTypeConfig::Id,
            DeviceTypeConfig::TypeId,
            DeviceTypeConfig::Name,
            DeviceTypeConfig::Category,
            DeviceTypeConfig::Type,
            DeviceTypeConfig::Value
        ])
        .from(DeviceConfig::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(DeviceTypeConfig::Id).eq(id)).to_owned();
    }
    else if let Some(type_id) = type_id {
        stmt = stmt.and_where(Expr::col(DeviceTypeConfig::TypeId).eq(type_id)).to_owned();
    }

    let stmt = stmt
        .order_by(DeviceTypeConfig::TypeId, Order::Asc)
        .order_by(DeviceTypeConfig::Id, Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_device_type_config(
    type_id: Uuid,
    name: &str,
    value_type: DataType,
    value_default: DataValue,
    category: &str
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(DeviceTypeConfig::Table)
        .columns([
            DeviceTypeConfig::TypeId,
            DeviceTypeConfig::Name,
            DeviceTypeConfig::Type,
            DeviceTypeConfig::Value,
            DeviceTypeConfig::Category
        ])
        .values([
            type_id.into(),
            name.into(),
            i16::from(value_type).into(),
            value_default.to_bytes().into(),
            category.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(DeviceTypeConfig::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_device_type_config(
    id: i32,
    name: Option<&str>,
    value_type: Option<DataType>,
    value_default: Option<DataValue>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(DeviceTypeConfig::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(DeviceTypeConfig::Name, value).to_owned();
    }
    if let Some(value) = value_type {
        stmt = stmt.value(DeviceTypeConfig::Name, i16::from(value)).to_owned();
    }
    if let Some(value) = value_default {
        stmt = stmt.value(DeviceTypeConfig::Name, value.to_bytes()).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(DeviceTypeConfig::Category, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(DeviceTypeConfig::TypeId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_device_type_config(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(DeviceTypeConfig::Table)
        .and_where(Expr::col(DeviceTypeConfig::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}
