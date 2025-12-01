use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum ProfileRole {
    Table,
    Id,
    RoleId,
    Name,
    Type,
    Mode
}

#[derive(Iden)]
pub(crate) enum ProfileUser {
    Table,
    Id,
    UserId,
    Name,
    Order,
    Value,
    Type
}
