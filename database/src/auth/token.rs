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
    Created,
    Expired,
    Ip
}

pub enum TokenSelector {
    Access(i32),
    Auth(String),
    User(Uuid),
    CreatedEarlier(DateTime<Utc>, Option<Uuid>),
    CreatedLater(DateTime<Utc>, Option<Uuid>),
    CreatedRange(DateTime<Utc>, DateTime<Utc>, Option<Uuid>),
    ExpiredEarlier(DateTime<Utc>, Option<Uuid>),
    ExpiredLater(DateTime<Utc>, Option<Uuid>),
    ExpiredRange(DateTime<Utc>, DateTime<Utc>, Option<Uuid>),
    Range(DateTime<Utc>, DateTime<Utc>, DateTime<Utc>, DateTime<Utc>, Option<Uuid>)
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
            Token::Created,
            Token::Expired,
            Token::Ip
        ])
        .from(Token::Table)
        .to_owned();

    let mut user_id: Option<Uuid> = None;
    match selector {
        TokenSelector::Access(value) => {
            stmt = stmt.and_where(Expr::col(Token::AccessId).eq(value)).to_owned();
        },
        TokenSelector::Auth(value) => {
            stmt = stmt.and_where(Expr::col(Token::AuthToken).eq(value)).to_owned();
        },
        TokenSelector::User(value) => {
            user_id = Some(value);
        },
        TokenSelector::CreatedEarlier(earlier, id) => {
            stmt = stmt.and_where(Expr::col(Token::Created).lte(earlier)).to_owned();
            user_id = id;
        },
        TokenSelector::CreatedLater(later, id) => {
            stmt = stmt.and_where(Expr::col(Token::Created).gte(later)).to_owned();
            user_id = id;
        },
        TokenSelector::CreatedRange(begin, end, id) => {
            stmt = stmt.and_where(Expr::col(Token::Created).gte(begin)).and_where(Expr::col(Token::Created).lte(end)).to_owned();
            user_id = id;
        },
        TokenSelector::ExpiredEarlier(earlier, id) => {
            stmt = stmt.and_where(Expr::col(Token::Expired).lte(earlier)).to_owned();
            user_id = id;
        },
        TokenSelector::ExpiredLater(later, id) => {
            stmt = stmt.and_where(Expr::col(Token::Expired).gte(later)).to_owned();
            user_id = id;
        },
        TokenSelector::ExpiredRange(begin, end, id) => {
            stmt = stmt.and_where(Expr::col(Token::Expired).gte(begin)).and_where(Expr::col(Token::Expired).lte(end)).to_owned();
            user_id = id;
        },
        TokenSelector::Range(b_cre, e_cre, b_exp, e_exp, id) => {
            stmt = stmt
                .and_where(Expr::col(Token::Created).gte(b_cre)).and_where(Expr::col(Token::Created).lte(e_cre))
                .and_where(Expr::col(Token::Expired).gte(b_exp)).and_where(Expr::col(Token::Expired).lte(e_exp))
                .to_owned();
            user_id = id;
        }
    }

    if let Some(value) = user_id {
        stmt = stmt.and_where(Expr::col(Token::UserId).eq(value)).to_owned();
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
    access_ids: Vec<i32>,
    refresh_tokens: Vec<&str>,
    auth_tokens: Vec<&str>,
    expired: DateTime<Utc>, 
    ip: &[u8]
) -> QuerySet
{
    let numbers = vec![access_ids.len(), refresh_tokens.len(), auth_tokens.len()];
    let number = numbers.into_iter().min().unwrap_or(0);

    let mut stmt = Query::insert()
        .into_table(Token::Table)
        .columns([
            Token::AccessId,
            Token::UserId,
            Token::RefreshToken,
            Token::AuthToken,
            Token::Expired,
            Token::Ip
        ])
        .to_owned();
    for i in 0..number {
        stmt = stmt.values([
            access_ids[i].into(),
            user_id.into(),
            refresh_tokens[i].into(),
            auth_tokens[i].into(),
            expired.into(),
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
    expired: Option<DateTime<Utc>>, 
    ip: Option<&[u8]>
) -> QuerySet
{
    let mut stmt = Query::update()
        .table(Token::Table)
        .to_owned();

    if let Some(value) = refresh_token {
        stmt = stmt.value(Token::RefreshToken, value).to_owned();
    }
    if let Some(value) = expired {
        stmt = stmt.value(Token::Expired, value).to_owned();
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
        _ => {}
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
        },
        _ => {}
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}
