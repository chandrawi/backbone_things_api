use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::auth::role::{Role, RoleAccess};

#[derive(Iden)]
pub(crate) enum Api {
    Table,
    ApiId,
    Name,
    Address,
    Category,
    Description,
    Password,
    AccessKey
}

#[derive(Iden)]
pub(crate) enum ApiProcedure {
    Table,
    ApiId,
    ProcedureId,
    Name,
    Description
}

pub fn select_api(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name_exact: Option<&str>,
    name_like: Option<&str>,
    category: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (Api::Table, Api::ApiId),
            (Api::Table, Api::Name),
            (Api::Table, Api::Address),
            (Api::Table, Api::Category),
            (Api::Table, Api::Description),
            (Api::Table, Api::Password),
            (Api::Table, Api::AccessKey)
        ])
        .columns([
            (ApiProcedure::Table, ApiProcedure::ProcedureId),
            (ApiProcedure::Table, ApiProcedure::Name),
            (ApiProcedure::Table, ApiProcedure::Description)
        ])
        .columns([
            (Role::Table, Role::Name)
        ])
        .from(Api::Table)
        .left_join(ApiProcedure::Table, 
            Expr::col((Api::Table, Api::ApiId))
            .equals((ApiProcedure::Table, ApiProcedure::ApiId))
        )
        .left_join(RoleAccess::Table, 
            Expr::col((ApiProcedure::Table, ApiProcedure::ProcedureId))
            .equals((RoleAccess::Table, RoleAccess::ProcedureId))
        )
        .left_join(Role::Table, 
            Expr::col((RoleAccess::Table, RoleAccess::RoleId))
            .equals((Role::Table, Role::RoleId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Api::Table, Api::ApiId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Api::Table, Api::ApiId)).is_in(ids.to_vec())).to_owned();
    }
    else if let Some(name) = name_exact {
        stmt = stmt.and_where(Expr::col((Api::Table, Api::Name)).eq(name.to_owned())).to_owned();
    }
    else {
        if let Some(name) = name_like {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Api::Table, Api::Name)).like(name_like)).to_owned();
        }
        if let Some(category) = category {
            let category_like = String::from("%") + category + "%";
            stmt = stmt.and_where(Expr::col((Api::Table, Api::Category)).like(category_like)).to_owned();
        }
    }

    stmt = stmt
        .order_by((Api::Table, Api::ApiId), Order::Asc)
        .order_by((ApiProcedure::Table, ApiProcedure::ProcedureId), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_api(
    id: Uuid,
    name: &str, 
    address: &str, 
    category: &str, 
    description: &str,
    password_hash: &str,
    access_key: &[u8]
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(Api::Table)
        .columns([
            Api::ApiId,
            Api::Name,
            Api::Address,
            Api::Category,
            Api::Description,
            Api::Password,
            Api::AccessKey
        ])
        .values([
            id.into(),
            name.into(),
            address.into(),
            category.into(),
            description.into(),
            password_hash.into(),
            access_key.to_vec().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_api(
    id: Uuid, 
    name: Option<&str>, 
    address: Option<&str>, 
    category: Option<&str>, 
    description: Option<&str>,
    password_hash: Option<&str>,
    access_key: Option<&[u8]>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(Api::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(Api::Name, value).to_owned();
    }
    if let Some(value) = address {
        stmt = stmt.value(Api::Address, value).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(Api::Category, value).to_owned();
    }
    if let Some(value) = password_hash {
        stmt = stmt.value(Api::Password, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Api::Description, value).to_owned();
    }
    if let Some(value) = access_key {
        stmt = stmt
            .value(Api::AccessKey, value.to_vec())
            .to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(Api::ApiId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_api(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(Api::Table)
        .and_where(Expr::col(Api::ApiId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_procedure(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    api_id: Option<Uuid>,
    name_exact: Option<&str>,
    name_like: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (ApiProcedure::Table, ApiProcedure::ProcedureId),
            (ApiProcedure::Table, ApiProcedure::ApiId),
            (ApiProcedure::Table, ApiProcedure::Name),
            (ApiProcedure::Table, ApiProcedure::Description)
        ])
        .columns([
            (Role::Table, Role::Name)
        ])
        .from(ApiProcedure::Table)
        .left_join(RoleAccess::Table, 
            Expr::col((ApiProcedure::Table, ApiProcedure::ProcedureId))
            .equals((RoleAccess::Table, RoleAccess::ProcedureId))
        )
        .left_join(Role::Table, 
            Expr::col((RoleAccess::Table, RoleAccess::RoleId))
            .equals((Role::Table, Role::RoleId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((ApiProcedure::Table, ApiProcedure::ProcedureId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((ApiProcedure::Table, ApiProcedure::ProcedureId)).is_in(ids.to_vec())).to_owned();
    }
    else if let (Some(api_id), Some(name)) = (api_id, name_exact) {
        stmt = stmt
            .and_where(Expr::col((ApiProcedure::Table, ApiProcedure::ApiId)).eq(api_id))
            .and_where(Expr::col((ApiProcedure::Table, ApiProcedure::Name)).eq(name.to_owned()))
            .to_owned();
    }
    else {
        if let Some(api_id) = api_id {
            stmt = stmt.and_where(Expr::col((ApiProcedure::Table, ApiProcedure::ApiId)).eq(api_id)).to_owned();
        }
        if let Some(name) = name_like {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((ApiProcedure::Table, ApiProcedure::Name)).like(name_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by(ApiProcedure::ProcedureId, Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_procedure(
    id: Uuid,
    api_id: Uuid,
    name: &str,
    description: &str
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(ApiProcedure::Table)
        .columns([
            ApiProcedure::ProcedureId,
            ApiProcedure::ApiId,
            ApiProcedure::Name,
            ApiProcedure::Description
        ])
        .values([
            id.into(),
            api_id.into(),
            name.into(),
            description.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_procedure(
    id: Uuid,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(ApiProcedure::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ApiProcedure::Name, value).to_owned()
    }
    if let Some(value) = description {
        stmt = stmt.value(ApiProcedure::Description, value).to_owned()
    }

    let stmt = stmt
        .and_where(Expr::col(ApiProcedure::ProcedureId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_procedure(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(ApiProcedure::Table)
        .and_where(Expr::col(ApiProcedure::ProcedureId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}
