use sea_query::{Iden, PostgresQueryBuilder, Query, Expr, Order, Condition};
use sea_query_binder::SqlxBinder;
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::QuerySet;
use crate::value::{DataValue, ArrayDataValue};
use crate::resource::model::Model;
use crate::resource::set::SetMap;

#[derive(Iden)]
pub(crate) enum Data {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Tag,
    Data
}

pub enum DataSelector {
    Time(DateTime<Utc>),
    Latest(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, usize),
    NumberAfter(DateTime<Utc>, usize)
}

pub fn select_data(
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Tag),
            (Data::Table, Data::Data)
        ])
        .column((Model::Table, Model::DataType))
        .from(Data::Table)
        .inner_join(Model::Table, 
            Expr::col((Data::Table, Data::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .to_owned();

    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else if device_ids.len() > 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else if model_ids.len() > 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
    }

    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).eq(time)).to_owned();
        },
        DataSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::NumberBefore(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Desc)
                .limit(limit as u64)
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .limit(limit as u64)
                .to_owned();
        }
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_data_timestamp(
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .column((Data::Table, Data::Timestamp))
        .from(Data::Table)
        .to_owned();

    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else if device_ids.len() > 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else if model_ids.len() > 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
    }

    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).eq(time)).to_owned();
        },
        DataSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last))
            .order_by((Data::Table, Data::Timestamp), Order::Asc)
            .to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        }
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_data_types(
    model_ids: &[Uuid]
) -> QuerySet
{
    let (query, values) = Query::select()
        .column((Model::Table, Model::DataType))
        .from(Model::Table)
        .and_where(Expr::col((Model::Table, Model::ModelId)).is_in(model_ids.to_vec()))
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_data(
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: &[DataValue],
    tag: Option<i16>
) -> QuerySet
{
    let bytes = ArrayDataValue::from_vec(data).to_bytes();
    let tag = tag.unwrap_or(0);
    let stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Tag,
            Data::Data
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            tag.into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_data_multiple(
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    timestamps: &[DateTime<Utc>],
    data: &[&[DataValue]],
    tags: Option<&[i16]>
) -> QuerySet
{
    let numbers = vec![device_ids.len(), model_ids.len(), timestamps.len(), data.len()];
    let number = numbers.into_iter().min().unwrap_or(0);
    let tags: Vec<i16> = match tags {
        Some(values) => (0..number).into_iter().map(|i| values.get(i).unwrap_or(&0).to_owned()).collect(),
        None => (0..number).into_iter().map(|_| 0).collect()
    };

    let mut stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Tag,
            Data::Data
        ])
        .to_owned();
    for i in 0..number {
        let bytes = ArrayDataValue::from_vec(&data[i]).to_bytes();
        stmt = stmt.values([
            device_ids[i].into(),
            model_ids[i].into(),
            timestamps[i].into(),
            tags[i].into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn delete_data(
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    tag: Option<i16>
) -> QuerySet
{
    let mut stmt = Query::delete()
        .from_table(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model_id))
        .and_where(Expr::col(Data::Timestamp).eq(timestamp))
        .to_owned();
    if let Some(t) = tag {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).eq(t)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_data_set(
    selector: DataSelector,
    set_id: Uuid,
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Tag),
            (Data::Table, Data::Data)
        ])
        .column((Model::Table, Model::DataType))
        .columns([
            (SetMap::Table, SetMap::DataIndex),
            (SetMap::Table, SetMap::SetPosition),
            (SetMap::Table, SetMap::SetNumber)
        ])
        .from(Data::Table)
        .inner_join(Model::Table, 
            Expr::col((Data::Table, Data::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .inner_join(SetMap::Table, 
            Condition::all()
            .add(Expr::col((Data::Table, Data::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
            .add(Expr::col((Data::Table, Data::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
        )
        .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id))
        .to_owned();

    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).eq(time)).to_owned();
        },
        DataSelector::Latest(last) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt
        .order_by((Data::Table, Data::Tag), Order::Asc)
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn count_data(
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .expr(Expr::col((Data::Table, Data::Timestamp)).count())
        .from(Data::Table)
        .to_owned();

    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
    }

    match selector {
        DataSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col(Data::Timestamp).gt(last)).to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(Data::Timestamp).gte(begin))
                .and_where(Expr::col(Data::Timestamp).lte(end))
                .to_owned();
        },
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}
