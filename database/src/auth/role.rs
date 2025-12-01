use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Role {
    Table,
    RoleId,
    ApiId,
    Name,
    Multi,
    IpLock,
    AccessDuration,
    RefreshDuration
}

#[derive(Iden)]
pub(crate) enum RoleAccess {
    Table,
    RoleId,
    ProcedureId
}
