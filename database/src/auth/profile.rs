use sea_query::{Iden, Query, Expr, Func};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::common::type_value::{DataType, DataValue};
use crate::auth::_schema::ProfileMode;

#[derive(Iden)]
pub(crate) enum ProfileRole {
    Table,
    Id,
    RoleId,
    Name,
    Type,
    Mode
}

#[derive(Iden)]
pub(crate) enum ProfileUser {
    Table,
    Id,
    UserId,
    Name,
    Order,
    Value,
    Type
}

pub fn select_role_profile(
    id: Option<i32>,
    role_id: Option<Uuid>,
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (ProfileRole::Table, ProfileRole::Id),
            (ProfileRole::Table, ProfileRole::RoleId),
            (ProfileRole::Table, ProfileRole::Name),
            (ProfileRole::Table, ProfileRole::Type),
            (ProfileRole::Table, ProfileRole::Mode)
        ])
        .from(ProfileRole::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((ProfileRole::Table, ProfileRole::Id)).eq(id)).to_owned();
    }
    else if let Some(role_id) = role_id {
        stmt = stmt.and_where(Expr::col((ProfileRole::Table, ProfileRole::RoleId)).eq(role_id)).to_owned();
    }

    QueryStatement::Select(stmt)
}

pub fn insert_role_profile(
    role_id: Uuid,
    name: &str,
    value_type: DataType,
    mode: ProfileMode
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(ProfileRole::Table)
        .columns([
            ProfileRole::RoleId,
            ProfileRole::Name,
            ProfileRole::Type,
            ProfileRole::Mode
        ])
        .values([
            role_id.into(),
            name.into(),
            i16::from(value_type).into(),
            i16::from(mode).into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(ProfileRole::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_role_profile(
    id: i32,
    name: Option<&str>,
    value_type: Option<DataType>,
    mode: Option<ProfileMode>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(ProfileRole::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ProfileRole::Name, value).to_owned();
    }
    if let Some(value) = value_type {
        let value_type = i16::from(value);
        stmt = stmt.value(ProfileRole::Type, value_type).to_owned();
    }
    if let Some(value) = mode {
        let mode = i16::from(value);
        stmt = stmt.value(ProfileRole::Mode, mode).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(ProfileRole::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_role_profile(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(ProfileRole::Table)
        .and_where(Expr::col(ProfileRole::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_user_profile(
    id: Option<i32>,
    user_id: Option<Uuid>,
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (ProfileUser::Table, ProfileUser::Id),
            (ProfileUser::Table, ProfileUser::UserId),
            (ProfileUser::Table, ProfileUser::Name),
            (ProfileUser::Table, ProfileUser::Order),
            (ProfileUser::Table, ProfileUser::Value),
            (ProfileUser::Table, ProfileUser::Type)
        ])
        .from(ProfileUser::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((ProfileUser::Table, ProfileUser::Id)).eq(id)).to_owned();
    }
    else if let Some(user_id) = user_id {
        stmt = stmt.and_where(Expr::col((ProfileUser::Table, ProfileUser::UserId)).eq(user_id)).to_owned();
    }

    QueryStatement::Select(stmt)
}

pub fn select_user_profile_max_order(
    user_id: Uuid,
    name: &str,
) -> QueryStatement
{
    let stmt = Query::select()
        .expr(Func::max(Expr::col(ProfileUser::Order)))
        .and_where(Expr::col(ProfileUser::UserId).eq(user_id))
        .and_where(Expr::col(ProfileUser::Name).eq(name))
        .from(ProfileUser::Table)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_user_profile(
    user_id: Uuid,
    name: &str,
    value: DataValue,
    order: i16
) -> QueryStatement
{
    let bytes = value.to_bytes();
    let type_ = i16::from(value.get_type());
    let stmt = Query::insert()
        .into_table(ProfileUser::Table)
        .columns([
            ProfileUser::UserId,
            ProfileUser::Name,
            ProfileUser::Order,
            ProfileUser::Value,
            ProfileUser::Type
        ])
        .values([
            user_id.into(),
            name.into(),
            order.into(),
            bytes.into(),
            type_.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(ProfileUser::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_user_profile(
    id: i32,
    name: Option<&str>,
    value: Option<DataValue>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(ProfileUser::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ProfileUser::Name, value).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(ProfileUser::Value, bytes)
            .value(ProfileUser::Type, type_)
            .to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(ProfileUser::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_user_profile(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(ProfileUser::Table)
        .and_where(Expr::col(ProfileUser::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn update_user_profile_order(
    user_id: Uuid,
    name: &str,
    order: i16,
    order_new: i16
) -> QueryStatement
{
    let stmt = Query::update()
        .table(ProfileUser::Table)
        .value(ProfileUser::Order, order_new).to_owned()
        .and_where(Expr::col(ProfileUser::UserId).eq(user_id))
        .and_where(Expr::col(ProfileUser::Name).eq(name))
        .and_where(Expr::col(ProfileUser::Order).eq(order))
        .to_owned();

    QueryStatement::Update(stmt)
}
