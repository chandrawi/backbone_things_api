use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Api {
    Table,
    ApiId,
    Name,
    Address,
    Category,
    Description,
    Password,
    AccessKey
}

#[derive(Iden)]
pub(crate) enum ApiProcedure {
    Table,
    ApiId,
    ProcedureId,
    Name,
    Description
}
