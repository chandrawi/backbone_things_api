use sea_query::{Condition, Expr, Func, Iden, Order, PostgresQueryBuilder, Query};
use sea_query_binder::SqlxBinder;
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::{QuerySet, tag as Tag};
use crate::value::{DataValue, ArrayDataValue};
use crate::resource::model::Model;
use crate::resource::set::SetMap;

#[derive(Iden)]
pub(crate) enum DataBuffer {
    Table,
    Id,
    DeviceId,
    ModelId,
    Timestamp,
    Tag,
    Data
}

pub enum BufferSelector {
    Time(DateTime<Utc>),
    Latest(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, usize),
    NumberAfter(DateTime<Utc>, usize),
    First(usize, usize),
    Last(usize, usize),
    None
}

pub fn select_buffer(
    selector: BufferSelector,
    ids: Option<&[i32]>,
    device_ids: Option<&[Uuid]>,
    model_ids: Option<&[Uuid]>,
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .columns([
            (DataBuffer::Table, DataBuffer::Id),
            (DataBuffer::Table, DataBuffer::DeviceId),
            (DataBuffer::Table, DataBuffer::ModelId),
            (DataBuffer::Table, DataBuffer::Timestamp),
            (DataBuffer::Table, DataBuffer::Tag),
            (DataBuffer::Table, DataBuffer::Data)
        ])
        .column((Model::Table, Model::DataType))
        .from(DataBuffer::Table)
        .inner_join(Model::Table, 
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .to_owned();

    if let Some(ids) = ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(ids[0])).to_owned();
        } else {
            stmt = stmt.and_where(Expr::col(DataBuffer::Id).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(ids) = device_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(ids) = model_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(ids.to_vec())).to_owned();
        }
    }

    match selector {
        BufferSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(timestamp)).to_owned();
        },
        BufferSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gt(last))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(begin))
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(end))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::NumberBefore(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Desc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::NumberAfter(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::None => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_buffer_timestamp(
    selector: BufferSelector,
    device_ids: Option<&[Uuid]>,
    model_ids: Option<&[Uuid]>,
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .column((DataBuffer::Table, DataBuffer::Timestamp))
        .from(DataBuffer::Table)
        .to_owned();

    if let Some(ids) = device_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(ids) = model_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(ids.to_vec())).to_owned();
        }
    }

    match selector {
        BufferSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(timestamp)).to_owned();
        },
        BufferSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gt(last))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(begin))
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(end))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_buffer_types(
    buffer_id: i32
) -> QuerySet
{
    let (query, values) = Query::select()
        .columns([
            (Model::Table, Model::DataType)
        ])
        .from(DataBuffer::Table)
        .inner_join(Model::Table,
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .and_where(Expr::col((DataBuffer::Table, DataBuffer::Id)).eq(buffer_id))
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_buffer_last_id(
) -> QuerySet
{
    let (query, values) = Query::select()
        .expr(Func::max(Expr::col(DataBuffer::Id)))
        .from(DataBuffer::Table)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_buffer(
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: &[DataValue],
    tag: Option<i16>
) -> QuerySet
{
    let bytes = ArrayDataValue::from_vec(data).to_bytes();
    let tag = tag.unwrap_or(Tag::DEFAULT);
    let (query, values) = Query::insert()
        .into_table(DataBuffer::Table)
        .columns([
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Tag,
            DataBuffer::Data
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            tag.into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn insert_buffer_multiple(
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
        Some(values) => (0..number).into_iter().map(|i| values.get(i).unwrap_or(&Tag::DEFAULT).to_owned()).collect(),
        None => (0..number).into_iter().map(|_| Tag::DEFAULT).collect()
    };

    let mut stmt = Query::insert()
        .into_table(DataBuffer::Table)
        .columns([
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Tag,
            DataBuffer::Data
        ])
        .to_owned();
    for i in 0..number {
        let bytes = ArrayDataValue::from_vec(&data[i]).to_bytes();
        stmt = stmt.values([
            device_ids[i].into(),
            model_ids[i].into(),
            timestamps[i].into(),
            tags[i].clone().into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn update_buffer(
    id: Option<i32>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    timestamp: Option<DateTime<Utc>>,
    data: Option<&[DataValue]>,
    tag: Option<i16>
) -> QuerySet
{
    let mut stmt = Query::update()
        .table(DataBuffer::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    }
    if let (Some(device_id), Some(model_id), Some(timestamp)) = (device_id, model_id, timestamp) {
        stmt = stmt
            .and_where(Expr::col(DataBuffer::DeviceId).eq(device_id))
            .and_where(Expr::col(DataBuffer::ModelId).eq(model_id))
            .and_where(Expr::col(DataBuffer::Timestamp).eq(timestamp))
            .to_owned();
        if let Some(tag) = tag {
            stmt = stmt.and_where(Expr::col(DataBuffer::Tag).eq(tag)).to_owned();
        }
    }

    if let (Some(tag), None, None, None) = (tag, device_id, model_id, timestamp) {
        stmt = stmt.value(DataBuffer::Tag, tag).to_owned();
    }
    if let Some(value) = data {
        let bytes = ArrayDataValue::from_vec(value).to_bytes();
        stmt = stmt.value(DataBuffer::Data, bytes).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn delete_buffer(
    id: Option<i32>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    timestamp: Option<DateTime<Utc>>,
    tag: Option<i16>
) -> QuerySet
{
    let mut stmt = Query::delete()
        .from_table(DataBuffer::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    }
    if let (Some(device_id), Some(model_id), Some(timestamp)) = (device_id, model_id, timestamp) {
        stmt = stmt
            .and_where(Expr::col(DataBuffer::DeviceId).eq(device_id))
            .and_where(Expr::col(DataBuffer::ModelId).eq(model_id))
            .and_where(Expr::col(DataBuffer::Timestamp).eq(timestamp))
            .to_owned();
        if let Some(tag) = tag {
            stmt = stmt.and_where(Expr::col(DataBuffer::Tag).eq(tag)).to_owned();
        }
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn select_buffer_set(
    selector: BufferSelector,
    set_id: Uuid,
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select().to_owned();
    stmt = stmt
        .columns([
            (DataBuffer::Table, DataBuffer::Id),
            (DataBuffer::Table, DataBuffer::DeviceId),
            (DataBuffer::Table, DataBuffer::ModelId),
            (DataBuffer::Table, DataBuffer::Timestamp),
            (DataBuffer::Table, DataBuffer::Tag),
            (DataBuffer::Table, DataBuffer::Data)
        ])
        .column((Model::Table, Model::DataType))
        .columns([
            (SetMap::Table, SetMap::DataIndex),
            (SetMap::Table, SetMap::SetPosition),
            (SetMap::Table, SetMap::SetNumber)
        ])
        .from(DataBuffer::Table)
        .inner_join(Model::Table, 
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .inner_join(SetMap::Table, 
            Condition::all()
            .add(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
            .add(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
        )
        .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id))
        .to_owned();

    match selector {
        BufferSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(timestamp)).to_owned();
        },
        BufferSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gt(last))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(begin))
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(end))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt
        .order_by((DataBuffer::Table, DataBuffer::Tag), Order::Asc)
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}

pub fn count_buffer(
    selector: BufferSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tags: Option<Vec<i16>>
) -> QuerySet
{
    let mut stmt = Query::select()
        .expr(Expr::col((DataBuffer::Table, DataBuffer::Id)).count())
        .from(DataBuffer::Table)
        .to_owned();

    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(model_ids.to_vec())).to_owned();
    }

    match selector {
        BufferSelector::Latest(last) => {
            stmt = stmt.and_where(Expr::col(DataBuffer::Timestamp).gt(last)).to_owned();
        },
        BufferSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(DataBuffer::Timestamp).gte(begin))
                .and_where(Expr::col(DataBuffer::Timestamp).lte(end))
                .to_owned();
        },
        _ => {}
    }

    if let Some(tags) = tags {
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }
    let (query, values) = stmt.build_sqlx(PostgresQueryBuilder);

    QuerySet { query, values }
}
