use sea_query::Iden;

#[derive(Iden)]
pub enum GroupModel {
    Table,
    Name,
    GroupId,
    Category,
    Description
}

#[derive(Iden)]
pub enum GroupModelMap {
    Table,
    GroupId,
    ModelId
}

#[derive(Iden)]
pub enum GroupDevice {
    Table,
    GroupId,
    Name,
    Kind,
    Category,
    Description
}

#[derive(Iden)]
pub enum GroupDeviceMap {
    Table,
    GroupId,
    DeviceId
}
