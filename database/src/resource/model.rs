use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Model {
    Table,
    ModelId,
    Category,
    Name,
    Description,
    DataType
}

#[derive(Iden)]
pub(crate) enum ModelTag {
    Table,
    ModelId,
    Tag,
    Name,
    Members
}

#[derive(Iden)]
pub(crate) enum ModelConfig {
    Table,
    Id,
    ModelId,
    Index,
    Name,
    Value,
    Type,
    Category
}
