use sea_query::{Iden, PostgresQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;
use crate::common::QuerySet;
use crate::auth::api::Api;
use crate::auth::role::Role;

#[derive(Iden)]
pub(crate) enum User {
    Table,
    UserId,
    Name,
    Password,
    Email,
    Phone
}

#[derive(Iden)]
pub(crate) enum UserRole {
    Table,
    UserId,
    RoleId
}

pub fn select_user(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    api_id: Option<Uuid>,
    role_id: Option<Uuid>,
    name_exact: Option<&str>,
    name_like: Option<&str>
) -> QuerySet
{
    let mut stmt = Query::select()
        .columns([
            (User::Table, User::UserId),
            (User::Table, User::Name),
            (User::Table, User::Password),
            (User::Table, User::Email),
            (User::Table, User::Phone)
        ])
        .columns([
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
        .from(User::Table)
        .left_join(UserRole::Table,
            Expr::col((User::Table, User::UserId))
            .equals((UserRole::Table, UserRole::UserId))
        )
        .left_join(Role::Table,
            Expr::col((UserRole::Table, UserRole::RoleId))
            .equals((Role::Table, Role::RoleId))
        )
        .left_join(Api::Table,
            Expr::col((Role::Table, Role::ApiId))
            .equals((Api::Table, Api::ApiId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((User::Table, User::UserId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((User::Table, User::UserId)).is_in(ids.to_vec())).to_owned();
    }
    else if let Some(name) = name_exact {
        stmt = stmt.and_where(Expr::col((User::Table, User::Name)).eq(name.to_owned())).to_owned();
    }
    else {
        if let Some(api_id) = api_id {
            stmt = stmt.and_where(Expr::col((Api::Table, Api::ApiId)).eq(api_id)).to_owned();
        }
        if let Some(role_id) = role_id {
            stmt = stmt.and_where(Expr::col((Role::Table, Role::RoleId)).eq(role_id)).to_owned();
        }
        if let Some(name) = name_like {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((User::Table, User::Name)).like(name_like)).to_owned();
        }
    }

    let (query, values) = stmt
        .order_by((User::Table, User::UserId), Order::Asc)
        .order_by((UserRole::Table, UserRole::RoleId), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_user(
    id: Uuid,
    name: &str, 
    email: &str,
    phone: &str,
    password_hash: &str
) -> QuerySet
{
    let (query, values) = Query::insert()
        .into_table(User::Table)
        .columns([
            User::UserId,
            User::Name,
            User::Password,
            User::Email,
            User::Phone
        ])
        .values([
            id.into(),
            name.into(),
            password_hash.into(),
            email.into(),
            phone.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn update_user(
    id: Uuid, 
    name: Option<&str>, 
    email: Option<&str>,
    phone: Option<&str>,
    password_hash: Option<&str>
) -> QuerySet
{
    let mut stmt = Query::update()
        .table(User::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(User::Name, value).to_owned();
    }
    if let Some(value) = email {
        stmt = stmt.value(User::Email, value).to_owned();
    }
    if let Some(value) = phone {
        stmt = stmt.value(User::Phone, value).to_owned();
    }
    if let Some(value) = password_hash {
        stmt = stmt.value(User::Password, value).to_owned();
    }

    let (query, values) = stmt
        .and_where(Expr::col(User::UserId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn delete_user(
    id: Uuid
) -> QuerySet
{
    let (query, values) = Query::delete()
        .from_table(User::Table)
        .and_where(Expr::col(User::UserId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_user_role(
    id: Uuid,
    role_id: Uuid
) -> QuerySet
{
    let (query, values) = Query::insert()
        .into_table(UserRole::Table)
        .columns([
            UserRole::UserId,
            UserRole::RoleId
        ])
        .values([
            id.into(),
            role_id.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn delete_user_role(
    id: Uuid,
    role_id: Uuid
) -> QuerySet
{
    let (query, values) = Query::delete()
        .from_table(UserRole::Table)
        .and_where(Expr::col(UserRole::UserId).eq(id))
        .and_where(Expr::col(UserRole::RoleId).eq(role_id))
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}
