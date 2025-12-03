use sea_query::{Iden, Query, Expr, Order, Func};
use uuid::Uuid;
use crate::common::QueryStatement;
use crate::value::{DataType, DataValue};
use crate::resource::device::DeviceTypeModel;
use crate::resource::set::SetMap;

#[derive(Iden)]
pub(crate) enum Model {
    Table,
    ModelId,
    Category,
    Name,
    Description,
    DataType
}

#[derive(Iden)]
pub(crate) enum ModelTag {
    Table,
    ModelId,
    Tag,
    Name,
    Members
}

#[derive(Iden)]
pub(crate) enum ModelConfig {
    Table,
    Id,
    ModelId,
    Index,
    Name,
    Value,
    Type,
    Category
}

pub fn select_model(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    type_id: Option<Uuid>,
    name: Option<&str>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::Name),
            (Model::Table, Model::Category),
            (Model::Table, Model::Description),
            (Model::Table, Model::DataType)
        ])
        .columns([
            (ModelTag::Table, ModelTag::Tag),
            (ModelTag::Table, ModelTag::Name),
            (ModelTag::Table, ModelTag::Members)
        ])
        .columns([
            (ModelConfig::Table, ModelConfig::Id),
            (ModelConfig::Table, ModelConfig::Index),
            (ModelConfig::Table, ModelConfig::Name),
            (ModelConfig::Table, ModelConfig::Value),
            (ModelConfig::Table, ModelConfig::Type),
            (ModelConfig::Table, ModelConfig::Category)
        ])
        .from(Model::Table)
        .left_join(ModelTag::Table, 
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelTag::Table, ModelTag::ModelId))
        )
        .left_join(ModelConfig::Table, 
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelConfig::Table, ModelConfig::ModelId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Model::Table, Model::ModelId)).eq(id)).to_owned()
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Model::Table, Model::ModelId)).is_in(ids.to_vec())).to_owned()
    }
    else {
        if let Some(type_id) = type_id {
            stmt = stmt.inner_join(DeviceTypeModel::Table, 
                    Expr::col((Model::Table, Model::ModelId))
                    .equals((DeviceTypeModel::Table, DeviceTypeModel::ModelId)))
                .and_where(Expr::col((DeviceTypeModel::Table, DeviceTypeModel::TypeId)).eq(type_id))
                .to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Name)).like(name_like)).to_owned();
        }
        if let Some(category) = category {
            let category_like = String::from("%") + category + "%";
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Category)).like(category_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .order_by((ModelTag::Table, ModelTag::Tag), Order::Asc)
        .order_by((ModelConfig::Table, ModelConfig::Id), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_model(
    id: Uuid,
    data_type: &[DataType],
    category: &str,
    name: &str,
    description: Option<&str>,
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(Model::Table)
        .columns([
            Model::ModelId,
            Model::Category,
            Model::Name,
            Model::Description,
            Model::DataType
        ])
        .values([
            id.into(),
            category.into(),
            name.into(),
            description.unwrap_or_default().into(),
            data_type.into_iter().map(|ty| {
                ty.to_owned().into()
            }).collect::<Vec<u8>>().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_model(
    id: Uuid,
    data_type: Option<&[DataType]>,
    category: Option<&str>,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(Model::Table)
        .to_owned();

    if let Some(value) = category {
        stmt = stmt.value(Model::Category, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Model::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Model::Description, value).to_owned();
    }
    if let Some(value) = data_type {
        stmt = stmt.value(Model::DataType, value.into_iter().map(|ty| {
            ty.to_owned().into()
        }).collect::<Vec<u8>>()).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(Model::ModelId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_model(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(Model::Table)
        .and_where(Expr::col(Model::ModelId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_model_config(
    id: Option<i32>,
    model_id: Option<Uuid>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            ModelConfig::Id,
            ModelConfig::ModelId,
            ModelConfig::Index,
            ModelConfig::Name,
            ModelConfig::Value,
            ModelConfig::Type,
            ModelConfig::Category
        ])
        .from(ModelConfig::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(ModelConfig::Id).eq(id)).to_owned();
    }
    else if let Some(model_id) = model_id {
        stmt = stmt.and_where(Expr::col(ModelConfig::ModelId).eq(model_id)).to_owned();
    }

    let stmt = stmt
        .order_by(ModelConfig::ModelId, Order::Asc)
        .order_by(ModelConfig::Index, Order::Asc)
        .order_by(ModelConfig::Id, Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn select_model_config_last_id(
) -> QueryStatement
{
    let stmt = Query::select()
        .expr(Func::max(Expr::col(ModelConfig::Id)))
        .from(ModelConfig::Table)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_model_config(
    model_id: Uuid,
    index: i32,
    name: &str,
    value: DataValue,
    category: &str
) -> QueryStatement
{
    let config_value = value.to_bytes();
    let config_type = i16::from(value.get_type());
    let stmt = Query::insert()
        .into_table(ModelConfig::Table)
        .columns([
            ModelConfig::ModelId,
            ModelConfig::Index,
            ModelConfig::Name,
            ModelConfig::Value,
            ModelConfig::Type,
            ModelConfig::Category
        ])
        .values([
            model_id.into(),
            index.into(),
            name.into(),
            config_value.into(),
            config_type.into(),
            category.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_model_config(
    id: i32,
    name: Option<&str>,
    value: Option<DataValue>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(ModelConfig::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ModelConfig::Name, value).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(ModelConfig::Value, bytes)
            .value(ModelConfig::Type, type_).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(ModelConfig::Category, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(ModelConfig::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_model_config(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(ModelConfig::Table)
        .and_where(Expr::col(ModelConfig::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_model_tag(
    model_id: Uuid,
    tag: Option<i16>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            ModelTag::ModelId,
            ModelTag::Tag,
            ModelTag::Name,
            ModelTag::Members
        ])
        .from(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .to_owned();

    if let Some(t) = tag {
        stmt = stmt.and_where(Expr::col(ModelTag::Tag).eq(t)).to_owned();
    }
    let stmt = stmt
        .order_by(ModelTag::Tag, Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn select_tag_members(
    model_ids: &[Uuid],
    tag: i16
) -> QueryStatement
{
    let stmt = Query::select()
        .column(ModelTag::Members)
        .from(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).is_in(model_ids.to_vec()))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn select_tag_members_set(
    set_id: Uuid,
    tag: i16
) -> QueryStatement
{
    let stmt = Query::select()
        .column(ModelTag::Members)
        .from(ModelTag::Table)
        .inner_join(SetMap::Table, 
            Expr::col((ModelTag::Table, ModelTag::ModelId))
            .equals((SetMap::Table, SetMap::ModelId)))
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_model_tag(
    model_id: Uuid,
    tag: i16,
    name: &str,
    members: &[i16]
) -> QueryStatement
{
    let mut bytes: Vec<u8> = Vec::new();
    for member in members {
        bytes.append(member.to_be_bytes().to_vec().as_mut());
    }
    let stmt = Query::insert()
        .into_table(ModelTag::Table)
        .columns([
            ModelTag::ModelId,
            ModelTag::Tag,
            ModelTag::Name,
            ModelTag::Members
        ])
        .values([
            model_id.into(),
            tag.into(),
            name.into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_model_tag(
    model_id: Uuid,
    tag: i16,
    name: Option<&str>,
    members: Option<&[i16]>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(ModelTag::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ModelTag::Name, value).to_owned();
    }
    if let Some(value) = members {
        let mut bytes: Vec<u8> = Vec::new();
        for member in value {
            bytes.append(member.to_be_bytes().to_vec().as_mut());
        }
        stmt = stmt.value(ModelTag::Members, bytes).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_model_tag(
    model_id: Uuid,
    tag: i16
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .to_owned();

    QueryStatement::Delete(stmt)
}
