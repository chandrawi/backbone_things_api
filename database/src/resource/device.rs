use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Device {
    Table,
    DeviceId,
    GatewayId,
    TypeId,
    SerialNumber,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceType {
    Table,
    TypeId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceTypeModel {
    Table,
    TypeId,
    ModelId
}

#[derive(Iden)]
pub(crate) enum DeviceConfig {
    Table,
    Id,
    DeviceId,
    Name,
    Value,
    Type,
    Category
}
