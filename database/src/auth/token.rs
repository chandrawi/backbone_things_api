use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Token {
    Table,
    AccessId,
    UserId,
    RefreshToken,
    AuthToken,
    Expire,
    Ip
}
