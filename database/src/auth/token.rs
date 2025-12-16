use sea_query::{Iden, Query, Expr, Order};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;

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
) -> QueryStatement
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
    let stmt = stmt.order_by(Token::AccessId, Order::Asc).to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_token(
    user_id: Uuid, 
    refresh_tokens: Vec<&str>,
    auth_token: &str,
    expired: DateTime<Utc>, 
    ip: &[u8]
) -> QueryStatement
{
    let mut stmt = Query::insert()
        .into_table(Token::Table)
        .columns([
            Token::UserId,
            Token::RefreshToken,
            Token::AuthToken,
            Token::Expired,
            Token::Ip
        ])
        .to_owned();
    for i in 0..refresh_tokens.len() {
        stmt = stmt.values([
            user_id.into(),
            refresh_tokens[i].into(),
            auth_token.into(),
            expired.into(),
            ip.to_vec().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    stmt = stmt.returning(Query::returning().column(Token::AccessId)).to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_token(
    selector: TokenSelector,
    refresh_token: Option<&str>,
    expired: Option<DateTime<Utc>>, 
    ip: Option<&[u8]>
) -> QueryStatement
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

    QueryStatement::Update(stmt)
}

pub fn delete_token(
    selector: TokenSelector
) -> QueryStatement
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

    QueryStatement::Delete(stmt)
}
