use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum User {
    Table,
    UserId,
    Name,
    Password,
    Email,
    Phone
}

#[derive(Iden)]
pub(crate) enum UserRole {
    Table,
    UserId,
    RoleId
}
