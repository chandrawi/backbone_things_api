use sea_query::Iden;

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
