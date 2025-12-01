use sea_query::{Iden, PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::QuerySet;

#[derive(Iden)]
pub(crate) enum Token {
    Table,
    AccessId,
    UserId,
    RefreshToken,
    AuthToken,
    Expire,
    Ip
}

pub enum TokenSelector {
    Access(i32),
    Auth(String),
    User(Uuid)
}

pub fn select_token(
    selector: TokenSelector
) -> QuerySet
{
    let mut stmt = Query::select()
        .columns([
            Token::AccessId,
            Token::UserId,
            Token::RefreshToken,
            Token::AuthToken,
            Token::Expire,
            Token::Ip
        ])
        .from(Token::Table)
        .to_owned();

    match selector {
        TokenSelector::Access(value) => {
            stmt = stmt.and_where(Expr::col(Token::AccessId).eq(value)).to_owned();
        },
        TokenSelector::Auth(value) => {
            stmt = stmt.and_where(Expr::col(Token::AuthToken).eq(value)).to_owned();
        },
        TokenSelector::User(value) => {
            stmt = stmt.and_where(Expr::col(Token::UserId).eq(value)).to_owned();
        }
    }
    let (query, values) = stmt
        .order_by(Token::AccessId, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_token_last_access_id(
) -> QuerySet
{
    let (query, values) = Query::select()
        .expr(Func::max(Expr::col(Token::AccessId)))
        .from(Token::Table)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_token(
    user_id: Uuid, 
    access_id: Vec<i32>,
    refresh_token: Vec<&str>,
    auth_token: Vec<&str>,
    expire: DateTime<Utc>, 
    ip: &[u8]
) -> QuerySet
{
    let numbers = vec![access_id.len(), refresh_token.len(), auth_token.len()];
    let number = numbers.into_iter().min().unwrap_or(0);

    let mut stmt = Query::insert()
        .into_table(Token::Table)
        .columns([
            Token::AccessId,
            Token::UserId,
            Token::RefreshToken,
            Token::AuthToken,
            Token::Expire,
            Token::Ip
        ])
        .to_owned();
    for i in 0..number {
        stmt = stmt.values([
            access_id[i].into(),
            user_id.into(),
            refresh_token[i].into(),
            auth_token[i].into(),
            expire.into(),
            ip.to_vec().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn update_token(
    selector: TokenSelector,
    refresh_token: Option<&str>,
    expire: Option<DateTime<Utc>>, 
    ip: Option<&[u8]>
) -> QuerySet
{
    let mut stmt = Query::update()
        .table(Token::Table)
        .to_owned();

    if let Some(value) = refresh_token {
        stmt = stmt.value(Token::RefreshToken, value).to_owned();
    }
    if let Some(value) = expire {
        stmt = stmt.value(Token::Expire, value).to_owned();
    }
    if let Some(value) = ip {
        stmt = stmt.value(Token::Ip, value).to_owned();
    }

    match selector {
        TokenSelector::Access(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::AccessId)).eq(value)).to_owned();
        },
        TokenSelector::Auth(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::AuthToken)).eq(value)).to_owned();
        },
        TokenSelector::User(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::UserId)).eq(value)).to_owned();
        }
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn delete_token(
    selector: TokenSelector
) -> QuerySet
{
    let mut stmt = Query::delete()
        .from_table(Token::Table)
        .to_owned();
    match selector {
        TokenSelector::Access(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::AccessId)).eq(value)).to_owned();
        },
        TokenSelector::Auth(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::AuthToken)).eq(value)).to_owned();
        },
        TokenSelector::User(value) => {
            stmt = stmt.and_where(Expr::col((Token::Table, Token::UserId)).eq(value)).to_owned();
        }
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}
