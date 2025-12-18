use sea_query::{Iden, Query, Expr, Order};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::resource::_schema::{SetMember as SetMemberSchema, SetTemplateMember as SetTemplateMemberSchema};

#[derive(Iden)]
pub(crate) enum Set {
    Table,
    SetId,
    TemplateId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SetMember {
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
pub(crate) enum SetTemplateMember {
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
            (SetMember::Table, SetMember::DeviceId),
            (SetMember::Table, SetMember::ModelId),
            (SetMember::Table, SetMember::DataIndex)
        ])
        .from(Set::Table)
        .left_join(SetMember::Table, 
            Expr::col((Set::Table, Set::SetId))
            .equals((SetMember::Table, SetMember::SetId))
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
        .order_by((SetMember::Table, SetMember::SetPosition), Order::Asc)
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

pub fn insert_set_members(
    set_id: Uuid,
    members: &[SetMemberSchema]
) -> QueryStatement
{
    let mut stmt = Query::insert()
        .into_table(SetMember::Table)
        .columns([
            SetMember::SetId,
            SetMember::DeviceId,
            SetMember::ModelId,
            SetMember::DataIndex,
            SetMember::SetPosition,
            SetMember::SetNumber
        ])
        .to_owned();

    let number = members.iter().fold(0, |acc, e| acc + e.data_index.len());
    let mut pos = 0;
    for member in members.iter() {
        stmt = stmt.values([
            set_id.into(),
            member.device_id.into(),
            member.model_id.into(),
            member.data_index.to_owned().into(),
            (pos as i16).into(),
            (number as i16).into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
        pos += member.data_index.len();
    }

    QueryStatement::Insert(stmt)
}

pub fn delete_set_members(
    set_id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SetMember::Table)
        .and_where(Expr::col(SetMember::SetId).eq(set_id))
        .returning(Query::returning().columns([
            SetMember::DeviceId,
            SetMember::ModelId,
            SetMember::DataIndex,
            SetMember::SetPosition
        ]))
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
            (SetTemplateMember::Table, SetTemplateMember::TypeId),
            (SetTemplateMember::Table, SetTemplateMember::ModelId),
            (SetTemplateMember::Table, SetTemplateMember::DataIndex)
        ])
        .from(SetTemplate::Table)
        .left_join(SetTemplateMember::Table, 
            Expr::col((SetTemplate::Table, SetTemplate::TemplateId))
            .equals((SetTemplateMember::Table, SetTemplateMember::TemplateId))
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
        .order_by((SetTemplateMember::Table, SetTemplateMember::TemplateIndex), Order::Asc)
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

pub fn insert_set_template_members(
    template_id: Uuid,
    members: &[SetTemplateMemberSchema]
) -> QueryStatement
{
    let mut stmt = Query::insert()
        .into_table(SetTemplateMember::Table)
        .columns([
            SetTemplateMember::TemplateId,
            SetTemplateMember::TypeId,
            SetTemplateMember::ModelId,
            SetTemplateMember::DataIndex,
            SetTemplateMember::TemplateIndex
        ])
        .to_owned();

    for (i, member) in members.into_iter().enumerate() {
        stmt = stmt.values([
            template_id.into(),
            member.type_id.into(),
            member.model_id.into(),
            member.data_index.clone().into(),
            (i as i16).into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }

    QueryStatement::Insert(stmt)
}

pub fn delete_set_template_members(
    template_id: Uuid
) -> QueryStatement
{
    let stmt = Query::delete()
        .from_table(SetTemplateMember::Table)
        .and_where(Expr::col(SetTemplateMember::TemplateId).eq(template_id))
        .returning(Query::returning().columns([
            SetTemplateMember::TypeId,
            SetTemplateMember::ModelId,
            SetTemplateMember::DataIndex,
            SetTemplateMember::TemplateIndex
        ]))
        .to_owned();

    QueryStatement::Delete(stmt)
}
