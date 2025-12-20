use sea_query::{Iden, Query, Expr, Order};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;

#[derive(Iden)]
pub(crate) enum SliceData {
    Table,
    Id,
    DeviceId,
    ModelId,
    TimestampBegin,
    TimestampEnd,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SliceDataSet {
    Table,
    Id,
    SetId,
    TimestampBegin,
    TimestampEnd,
    Name,
    Description
}

pub enum SliceSelector {
    Time(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    None
}

pub fn select_slice(
    selector: SliceSelector,
    ids: Option<&[i32]>,
    device_ids: Option<&[Uuid]>,
    model_ids: Option<&[Uuid]>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            SliceData::Id,
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::Name,
            SliceData::Description
        ])
        .from(SliceData::Table)
        .to_owned();

    if let Some(ids) = ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col(SliceData::Id).eq(ids[0])).to_owned();
        } else {
            stmt = stmt.and_where(Expr::col(SliceData::Id).is_in(ids.to_vec())).to_owned();
        }
    }
    else {
        if let Some(ids) = device_ids {
            if ids.len() == 1 {
                stmt = stmt.and_where(Expr::col(SliceData::DeviceId).eq(ids[0])).to_owned();
            }
            else if ids.len() > 1 {
                stmt = stmt.and_where(Expr::col(SliceData::DeviceId).is_in(ids.to_vec())).to_owned();
            }
        }
        if let Some(ids) = model_ids {
            if ids.len() == 1 {
                stmt = stmt.and_where(Expr::col(SliceData::ModelId).eq(ids[0])).to_owned();
            }
            else if ids.len() > 1 {
                stmt = stmt.and_where(Expr::col(SliceData::ModelId).is_in(ids.to_vec())).to_owned();
            }
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col(SliceData::Name).like(name_like)).to_owned();
        }
        match selector {
            SliceSelector::Time(time) => {
                stmt = stmt
                    .and_where(Expr::col(SliceData::TimestampBegin).lte(time))
                    .and_where(Expr::col(SliceData::TimestampEnd).gte(time))
                    .to_owned();
            },
            SliceSelector::Range(begin, end) => {
                stmt = stmt
                    .and_where(Expr::col(SliceData::TimestampBegin).gte(begin))
                    .and_where(Expr::col(SliceData::TimestampEnd).lte(end))
                    .to_owned();
            }
            SliceSelector::None => {}
        }
        stmt = stmt.order_by(SliceData::Id, Order::Asc).to_owned();
    }

    QueryStatement::Select(stmt)
}

pub fn insert_slice(
    device_id: Uuid,
    model_id: Uuid,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    name: &str,
    description: &str
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(SliceData::Table)
        .columns([
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::Name,
            SliceData::Description
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp_begin.into(),
            timestamp_end.into(),
            name.into(),
            description.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(SliceData::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_slice(
    id: i32,
    timestamp_begin: Option<DateTime<Utc>>,
    timestamp_end: Option<DateTime<Utc>>,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(SliceData::Table)
        .to_owned();

    if let Some(timestamp) = timestamp_begin {
        stmt = stmt.value(SliceData::TimestampBegin, timestamp).to_owned();
    }
    if let Some(timestamp) = timestamp_end {
        stmt = stmt.value(SliceData::TimestampEnd, timestamp).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(SliceData::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(SliceData::Description, description).to_owned();
    }
    let stmt = stmt
        .and_where(Expr::col(SliceData::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_slice(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SliceData::Table)
        .and_where(Expr::col(SliceData::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_slice_set(
    selector: SliceSelector,
    ids: Option<&[i32]>,
    set_id: Option<Uuid>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            SliceDataSet::Id,
            SliceDataSet::SetId,
            SliceDataSet::TimestampBegin,
            SliceDataSet::TimestampEnd,
            SliceDataSet::Name,
            SliceDataSet::Description
        ])
        .from(SliceDataSet::Table)
        .to_owned();

    if let Some(ids) = ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col(SliceDataSet::Id).eq(ids[0])).to_owned();
        } else {
            stmt = stmt.and_where(Expr::col(SliceDataSet::Id).is_in(ids.to_vec())).to_owned();
        }
    }
    else {
        if let Some(id) = set_id {
            stmt = stmt.and_where(Expr::col(SliceDataSet::SetId).eq(id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col(SliceDataSet::Name).like(name_like)).to_owned();
        }
        match selector {
            SliceSelector::Time(time) => {
                stmt = stmt
                    .and_where(Expr::col(SliceDataSet::TimestampBegin).lte(time))
                    .and_where(Expr::col(SliceDataSet::TimestampEnd).gte(time))
                    .to_owned();
            },
            SliceSelector::Range(begin, end) => {
                stmt = stmt
                    .and_where(Expr::col(SliceDataSet::TimestampBegin).gte(begin))
                    .and_where(Expr::col(SliceDataSet::TimestampEnd).lte(end))
                    .to_owned();
            }
            SliceSelector::None => {}
        }
        stmt = stmt.order_by(SliceDataSet::Id, Order::Asc).to_owned();
    }

    QueryStatement::Select(stmt)
}

pub fn insert_slice_set(
    set_id: Uuid,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    name: &str,
    description: &str
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(SliceDataSet::Table)
        .columns([
            SliceDataSet::SetId,
            SliceDataSet::TimestampBegin,
            SliceDataSet::TimestampEnd,
            SliceDataSet::Name,
            SliceDataSet::Description
        ])
        .values([
            set_id.into(),
            timestamp_begin.into(),
            timestamp_end.into(),
            name.into(),
            description.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .returning(Query::returning().column(SliceDataSet::Id))
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_slice_set(
    id: i32,
    timestamp_begin: Option<DateTime<Utc>>,
    timestamp_end: Option<DateTime<Utc>>,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(SliceDataSet::Table)
        .to_owned();

    if let Some(timestamp) = timestamp_begin {
        stmt = stmt.value(SliceDataSet::TimestampBegin, timestamp).to_owned();
    }
    if let Some(timestamp) = timestamp_end {
        stmt = stmt.value(SliceDataSet::TimestampEnd, timestamp).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(SliceDataSet::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(SliceDataSet::Description, description).to_owned();
    }
    let stmt = stmt
        .and_where(Expr::col(SliceDataSet::Id).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_slice_set(
    id: i32
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SliceDataSet::Table)
        .and_where(Expr::col(SliceDataSet::Id).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}
