use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::auth::api::Api;
use crate::auth::user::UserRole;

#[derive(Iden)]
pub(crate) enum Role {
    Table,
    RoleId,
    ApiId,
    Name,
    Multi,
    IpLock,
    AccessDuration,
    RefreshDuration
}

#[derive(Iden)]
pub(crate) enum RoleAccess {
    Table,
    RoleId,
    ProcedureId
}

pub fn select_role(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    api_id: Option<Uuid>,
    user_id: Option<Uuid>,
    name_exact: Option<&str>,
    name_like: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (Role::Table, Role::RoleId),
            (Role::Table, Role::ApiId),
            (Role::Table, Role::Name),
            (Role::Table, Role::Multi),
            (Role::Table, Role::IpLock),
            (Role::Table, Role::AccessDuration),
            (Role::Table, Role::RefreshDuration)
        ])
        .columns([
            (Api::Table, Api::AccessKey)
        ])
        .columns([
            (RoleAccess::Table, RoleAccess::ProcedureId)
        ])
        .from(Role::Table)
        .inner_join(Api::Table, 
            Expr::col((Role::Table, Role::ApiId))
            .equals((Api::Table, Api::ApiId))
        )
        .left_join(RoleAccess::Table, 
            Expr::col((Role::Table, Role::RoleId))
            .equals((RoleAccess::Table, RoleAccess::RoleId))
        )
        .left_join(UserRole::Table,
            Expr::col((Role::Table, Role::RoleId))
            .equals((UserRole::Table, UserRole::RoleId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Role::Table, Role::RoleId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Role::Table, Role::RoleId)).is_in(ids.to_vec())).to_owned();
    }
    else if let (Some(api_id), Some(name)) = (api_id, name_exact) {
        stmt = stmt
            .and_where(Expr::col((Role::Table, Role::ApiId)).eq(api_id))
            .and_where(Expr::col((Role::Table, Role::Name)).eq(name.to_owned()))
            .to_owned();
    }
    else {
        if let Some(api_id) = api_id {
            stmt = stmt.and_where(Expr::col((Role::Table, Role::ApiId)).eq(api_id)).to_owned();
        }
        if let Some(user_id) = user_id {
            stmt = stmt.and_where(Expr::col((UserRole::Table, UserRole::UserId)).eq(user_id)).to_owned();
        }
        if let Some(name) = name_like {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Api::Table, Api::Name)).like(name_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by((Role::Table, Role::RoleId), Order::Asc)
        .order_by((RoleAccess::Table, RoleAccess::ProcedureId), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_role(
    id: Uuid,
    api_id: Uuid,
    name: &str, 
    multi: bool, 
    ip_lock: bool, 
    access_duration: i32,
    refresh_duration: i32,
) -> QueryStatement 
{
    let stmt = Query::insert()
        .into_table(Role::Table)
        .columns([
            Role::RoleId,
            Role::ApiId,
            Role::Name,
            Role::Multi,
            Role::IpLock,
            Role::AccessDuration,
            Role::RefreshDuration
        ])
        .values([
            id.into(),
            api_id.into(),
            name.into(),
            multi.into(),
            ip_lock.into(),
            access_duration.into(),
            refresh_duration.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_role(
    id: Uuid, 
    name: Option<&str>, 
    multi: Option<bool>, 
    ip_lock: Option<bool>, 
    access_duration: Option<i32>,
    refresh_duration: Option<i32>
) -> QueryStatement 
{
    let mut stmt = Query::update()
        .table(Role::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(Role::Name, value).to_owned();
    }
    if let Some(value) = multi {
        stmt = stmt.value(Role::Multi, value).to_owned();
    }
    if let Some(value) = ip_lock {
        stmt = stmt.value(Role::IpLock, value).to_owned();
    }
    if let Some(value) = access_duration {
        stmt = stmt.value(Role::AccessDuration, value).to_owned();
    }
    if let Some(value) = refresh_duration {
        stmt = stmt.value(Role::RefreshDuration, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(Role::RoleId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_role(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(Role::Table)
        .and_where(Expr::col(Role::RoleId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn insert_role_access(
    id: Uuid,
    procedure_id: Uuid
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(RoleAccess::Table)
        .columns([
            RoleAccess::RoleId,
            RoleAccess::ProcedureId
        ])
        .values([
            id.into(),
            procedure_id.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn delete_role_access(
    id: Uuid,
    procedure_id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(RoleAccess::Table)
        .and_where(Expr::col(RoleAccess::RoleId).eq(id))
        .and_where(Expr::col(RoleAccess::ProcedureId).eq(procedure_id))
        .to_owned();

    QueryStatement::Delete(stmt)
}
