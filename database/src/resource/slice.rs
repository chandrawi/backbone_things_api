use sea_query::Iden;

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
