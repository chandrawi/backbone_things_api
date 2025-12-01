use sea_query::Iden;

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
