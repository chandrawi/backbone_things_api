use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Data {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Tag,
    Data
}
