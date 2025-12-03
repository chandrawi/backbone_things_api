use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;

#[derive(Iden)]
pub(crate) enum GroupModel {
    Table,
    Name,
    GroupId,
    Category,
    Description
}

#[derive(Iden)]
pub(crate) enum GroupModelMap {
    Table,
    GroupId,
    ModelId
}

#[derive(Iden)]
pub(crate) enum GroupDevice {
    Table,
    GroupId,
    Name,
    Kind,
    Category,
    Description
}

#[derive(Iden)]
pub(crate) enum GroupDeviceMap {
    Table,
    GroupId,
    DeviceId
}

#[derive(Clone, PartialEq)]
pub enum GroupKind {
    Model,
    Device,
    Gateway
}

pub fn select_group(
    kind: GroupKind,
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name: Option<&str>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .columns([
                    (GroupModel::Table, GroupModel::GroupId),
                    (GroupModel::Table, GroupModel::Name),
                    (GroupModel::Table, GroupModel::Category),
                    (GroupModel::Table, GroupModel::Description)
                ])
                .columns([
                    (GroupModelMap::Table, GroupModelMap::ModelId)
                ])
                .from(GroupModel::Table)
                .left_join(GroupModelMap::Table, 
                    Expr::col((GroupModel::Table, GroupModel::GroupId))
                    .equals((GroupModelMap::Table, GroupModelMap::GroupId))
                )
                .to_owned();
            if let Some(id) = id {
                stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::GroupId)).eq(id)).to_owned();
            }
            else if let Some(ids) = ids {
                stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::GroupId)).is_in(ids.to_vec())).to_owned();
            }
            else {
                if let Some(name) = name {
                    let name_like = String::from("%") + name + "%";
                    stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::Name)).like(name_like)).to_owned();
                }
                if let Some(category) = category {
                    let category_like = String::from("%") + category + "%";
                    stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::Category)).like(category_like)).to_owned();
                }
            }
            stmt = stmt
                .order_by((GroupModel::Table, GroupModel::GroupId), Order::Asc)
                .order_by((GroupModelMap::Table, GroupModelMap::ModelId), Order::Asc)
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .columns([
                    (GroupDevice::Table, GroupDevice::GroupId),
                    (GroupDevice::Table, GroupDevice::Name),
                    (GroupDevice::Table, GroupDevice::Category),
                    (GroupDevice::Table, GroupDevice::Description)
                ])
                .columns([
                    (GroupDeviceMap::Table, GroupDeviceMap::DeviceId)
                ])
                .from(GroupDevice::Table)
                .left_join(GroupDeviceMap::Table, 
                    Expr::col((GroupDevice::Table, GroupDevice::GroupId))
                    .equals((GroupDeviceMap::Table, GroupDeviceMap::GroupId))
                )
                .and_where(Expr::col((GroupDevice::Table, GroupDevice::Kind)).eq(kind == GroupKind::Gateway)).to_owned()
                .to_owned();
            if let Some(id) = id {
                stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::GroupId)).eq(id)).to_owned();
            }
            else if let Some(ids) = ids {
                stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::GroupId)).is_in(ids.to_vec())).to_owned();
            }
            else {
                if let Some(name) = name {
                    let name_like = String::from("%") + name + "%";
                    stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::Name)).like(name_like)).to_owned();
                }
                if let Some(category) = category {
                    let category_like = String::from("%") + category + "%";
                    stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::Category)).like(category_like)).to_owned();
                }
            }
            stmt = stmt
                .order_by((GroupDevice::Table, GroupDevice::GroupId), Order::Asc)
                .order_by((GroupDeviceMap::Table, GroupDeviceMap::DeviceId), Order::Asc)
                .to_owned();
        }
    }

    QueryStatement::Select(stmt)
}

pub fn insert_group(
    kind: GroupKind,
    id: Uuid,
    name: &str,
    category: &str,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::insert().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .into_table(GroupModel::Table)
                .columns([
                    GroupModel::GroupId,
                    GroupModel::Name,
                    GroupModel::Category,
                    GroupModel::Description
                ])
                .values([
                    id.into(),
                    name.into(),
                    category.into(),
                    description.unwrap_or_default().into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .into_table(GroupDevice::Table)
                .columns([
                    GroupDevice::GroupId,
                    GroupDevice::Name,
                    GroupDevice::Kind,
                    GroupDevice::Category,
                    GroupDevice::Description
                ])
                .values([
                    id.into(),
                    name.into(),
                    (kind == GroupKind::Gateway).into(),
                    category.into(),
                    description.unwrap_or_default().into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        }
    }

    QueryStatement::Insert(stmt)
}

pub fn update_group(
    kind: GroupKind,
    id: Uuid,
    name: Option<&str>,
    category: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt.table(GroupModel::Table).to_owned();
            if let Some(value) = name {
                stmt = stmt.value(GroupModel::Name, value).to_owned();
            }
            if let Some(value) = category {
                stmt = stmt.value(GroupModel::Category, value).to_owned();
            }
            if let Some(value) = description {
                stmt = stmt.value(GroupModel::Description, value).to_owned();
            }
            stmt = stmt.and_where(Expr::col(GroupModel::GroupId).eq(id)).to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt.table(GroupDevice::Table).to_owned();
            if let Some(value) = name {
                stmt = stmt.value(GroupDevice::Name, value).to_owned();
            }
            if let Some(value) = category {
                stmt = stmt.value(GroupDevice::Category, value).to_owned();
            }
            if let Some(value) = description {
                stmt = stmt.value(GroupDevice::Description, value).to_owned();
            }
            stmt = stmt.and_where(Expr::col(GroupDevice::GroupId).eq(id)).to_owned();
        }
    }

    QueryStatement::Update(stmt)
}

pub fn delete_group(
    kind: GroupKind,
    id: Uuid
) -> QueryStatement
{
    let mut stmt = Query::delete().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .from_table(GroupModel::Table)
                .and_where(Expr::col(GroupModel::GroupId).eq(id))
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .from_table(GroupDevice::Table)
                .and_where(Expr::col(GroupDevice::GroupId).eq(id))
                .to_owned();
        }
    }

    QueryStatement::Delete(stmt)
}

pub fn insert_group_map(
    kind: GroupKind,
    id: Uuid,
    member_id: Uuid
) -> QueryStatement
{
    let mut stmt = Query::insert().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .into_table(GroupModelMap::Table)
                .columns([
                    GroupModelMap::GroupId,
                    GroupModelMap::ModelId
                ])
                .values([
                    id.into(),
                    member_id.into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .into_table(GroupDeviceMap::Table)
                .columns([
                    GroupDeviceMap::GroupId,
                    GroupDeviceMap::DeviceId
                ])
                .values([
                    id.into(),
                    member_id.into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        }
    }

    QueryStatement::Insert(stmt)
}

pub fn delete_group_map(
    kind: GroupKind,
    id: Uuid,
    member_id: Uuid
) -> QueryStatement
{
    let mut stmt = Query::delete().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .from_table(GroupModelMap::Table)
                .and_where(Expr::col(GroupModelMap::GroupId).eq(id))
                .and_where(Expr::col(GroupModelMap::ModelId).eq(member_id))
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .from_table(GroupDeviceMap::Table)
                .and_where(Expr::col(GroupDeviceMap::GroupId).eq(id))
                .and_where(Expr::col(GroupDeviceMap::DeviceId).eq(member_id))
                .to_owned();
        }
    }

    QueryStatement::Delete(stmt)
}
