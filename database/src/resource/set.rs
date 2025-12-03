use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::QueryStatement;

#[derive(Iden)]
pub(crate) enum Set {
    Table,
    SetId,
    TemplateId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SetMap {
    Table,
    SetId,
    DeviceId,
    ModelId,
    DataIndex,
    SetPosition,
    SetNumber
}

#[derive(Iden)]
pub(crate) enum SetTemplate {
    Table,
    TemplateId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SetTemplateMap {
    Table,
    TemplateId,
    TypeId,
    ModelId,
    DataIndex,
    TemplateIndex
}

pub fn select_set(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    template_id: Option<Uuid>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (Set::Table, Set::SetId),
            (Set::Table, Set::TemplateId),
            (Set::Table, Set::Name),
            (Set::Table, Set::Description)
        ])
        .columns([
            (SetMap::Table, SetMap::DeviceId),
            (SetMap::Table, SetMap::ModelId),
            (SetMap::Table, SetMap::DataIndex)
        ])
        .from(Set::Table)
        .left_join(SetMap::Table, 
            Expr::col((Set::Table, Set::SetId))
            .equals((SetMap::Table, SetMap::SetId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Set::Table, Set::SetId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Set::Table, Set::SetId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(template_id) = template_id {
            stmt = stmt.and_where(Expr::col((Set::Table, Set::TemplateId)).eq(template_id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Set::Table, Set::Name)).like(name_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by((Set::Table, Set::SetId), Order::Asc)
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_set(
    id: Uuid,
    template_id: Uuid,
    name: &str,
    description: Option<&str>,
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(Set::Table)
        .columns([
            Set::SetId,
            Set::TemplateId,
            Set::Name,
            Set::Description
        ])
        .values([
            id.into(),
            template_id.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_set(
    id: Uuid,
    template_id: Option<Uuid>,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(Set::Table)
        .to_owned();

    if let Some(value) = template_id {
        stmt = stmt.value(Set::TemplateId, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Set::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Set::Description, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(Set::SetId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_set(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(Set::Table)
        .and_where(Expr::col(Set::SetId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn read_set_members(
    set_id: Uuid
) -> QueryStatement
{
    let stmt = Query::select()
        .columns([
            (SetMap::Table, SetMap::DeviceId),
            (SetMap::Table, SetMap::ModelId),
            (SetMap::Table, SetMap::DataIndex),
        ])
        .from(SetMap::Table)
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn update_set_position_number(
    set_id: Uuid,
    device_id: Uuid,
    model_id: Uuid,
    position: Option<i16>,
    number: Option<i16>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(SetMap::Table)
        .to_owned();
    if let Some(pos) = position {
        stmt = stmt
            .value(SetMap::SetPosition, pos)
            .and_where(Expr::col(SetMap::DeviceId).eq(device_id))
            .and_where(Expr::col(SetMap::ModelId).eq(model_id))
            .to_owned();
    }
    if let Some(num) = number {
        stmt = stmt.value(SetMap::SetNumber, num).to_owned();
    }
    let stmt = stmt
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn insert_set_member(
    id: Uuid,
    device_id: Uuid,
    model_id: Uuid,
    data_index: &[u8],
    position: i16,
    number: i16
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(SetMap::Table)
        .columns([
            SetMap::SetId,
            SetMap::DeviceId,
            SetMap::ModelId,
            SetMap::DataIndex,
            SetMap::SetPosition,
            SetMap::SetNumber
        ])
        .values([
            id.into(),
            device_id.into(),
            model_id.into(),
            data_index.to_owned().into(),
            position.into(),
            number.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn delete_set_member(
    id: Uuid,
    device_id: Uuid,
    model_id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SetMap::Table)
        .and_where(Expr::col(SetMap::SetId).eq(id))
        .and_where(Expr::col(SetMap::DeviceId).eq(device_id))
        .and_where(Expr::col(SetMap::ModelId).eq(model_id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn select_set_template(
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::select()
        .columns([
            (SetTemplate::Table, SetTemplate::TemplateId),
            (SetTemplate::Table, SetTemplate::Name),
            (SetTemplate::Table, SetTemplate::Description)
        ])
        .columns([
            (SetTemplateMap::Table, SetTemplateMap::TypeId),
            (SetTemplateMap::Table, SetTemplateMap::ModelId),
            (SetTemplateMap::Table, SetTemplateMap::DataIndex)
        ])
        .from(SetTemplate::Table)
        .left_join(SetTemplateMap::Table, 
            Expr::col((SetTemplate::Table, SetTemplate::TemplateId))
            .equals((SetTemplateMap::Table, SetTemplateMap::TemplateId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::TemplateId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::TemplateId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::Name)).like(name_like)).to_owned();
        }
    }

    let stmt = stmt
        .order_by((SetTemplate::Table, SetTemplate::TemplateId), Order::Asc)
        .order_by((SetTemplateMap::Table, SetTemplateMap::TemplateIndex), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn insert_set_template(
    id: Uuid,
    name: &str,
    description: Option<&str>,
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(SetTemplate::Table)
        .columns([
            SetTemplate::TemplateId,
            SetTemplate::Name,
            SetTemplate::Description
        ])
        .values([
            id.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn update_set_template(
    id: Uuid,
    name: Option<&str>,
    description: Option<&str>
) -> QueryStatement
{
    let mut stmt = Query::update()
        .table(SetTemplate::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(SetTemplate::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(SetTemplate::Description, value).to_owned();
    }

    let stmt = stmt
        .and_where(Expr::col(SetTemplate::TemplateId).eq(id))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn delete_set_template(
    id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SetTemplate::Table)
        .and_where(Expr::col(SetTemplate::TemplateId).eq(id))
        .to_owned();

    QueryStatement::Delete(stmt)
}

pub fn read_set_template_members(
    template_id: Uuid
) -> QueryStatement
{
    let stmt = Query::select()
        .columns([
            (SetTemplateMap::Table, SetTemplateMap::TypeId),
            (SetTemplateMap::Table, SetTemplateMap::ModelId),
            (SetTemplateMap::Table, SetTemplateMap::DataIndex)
        ])
        .from(SetTemplateMap::Table)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(template_id))
        .order_by((SetTemplateMap::Table, SetTemplateMap::TemplateIndex), Order::Asc)
        .to_owned();

    QueryStatement::Select(stmt)
}

pub fn update_set_template_index(
    template_id: Uuid, 
    index: i16, 
    new_index: i16
) -> QueryStatement
{
    let stmt = Query::update()
        .table(SetTemplateMap::Table)
        .value(SetTemplateMap::TemplateIndex, new_index)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(template_id))
        .and_where(Expr::col(SetTemplateMap::TemplateIndex).eq(index))
        .to_owned();

    QueryStatement::Update(stmt)
}

pub fn insert_set_template_member(
    id: Uuid,
    type_id: Uuid,
    model_id: Uuid,
    data_index: &[u8],
    template_index: i16
) -> QueryStatement
{
    let stmt = Query::insert()
        .into_table(SetTemplateMap::Table)
        .columns([
            SetTemplateMap::TemplateId,
            SetTemplateMap::TypeId,
            SetTemplateMap::ModelId,
            SetTemplateMap::DataIndex,
            SetTemplateMap::TemplateIndex
        ])
        .values([
            id.into(),
            type_id.into(),
            model_id.into(),
            data_index.to_owned().into(),
            template_index.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();

    QueryStatement::Insert(stmt)
}

pub fn delete_set_template_member(
    id: Uuid,
    template_index: i16
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SetTemplateMap::Table)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(id))
        .and_where(Expr::col(SetTemplateMap::TemplateIndex).eq(template_index))
        .to_owned();

    QueryStatement::Delete(stmt)
}
