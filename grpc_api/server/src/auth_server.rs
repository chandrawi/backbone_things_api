use bbthings_database::Auth;
use bbthings_database::utility::migrate_auth;
use bbthings_grpc_server::proto::auth::api::api_service_server::ApiServiceServer;
use bbthings_grpc_server::proto::auth::role::role_service_server::RoleServiceServer;
use bbthings_grpc_server::proto::auth::user::user_service_server::UserServiceServer;
use bbthings_grpc_server::proto::auth::profile::profile_service_server::ProfileServiceServer;
use bbthings_grpc_server::proto::auth::token::token_service_server::TokenServiceServer;
use bbthings_grpc_server::proto::auth::auth::auth_service_server::AuthServiceServer;
use bbthings_grpc_server::proto::descriptor;
use bbthings_grpc_server::auth::api::ApiServer;
use bbthings_grpc_server::auth::role::RoleServer;
use bbthings_grpc_server::auth::user::UserServer;
use bbthings_grpc_server::auth::profile::ProfileServer;
use bbthings_grpc_server::auth::token::TokenServer;
use bbthings_grpc_server::auth::auth::AuthServer;
use bbthings_grpc_server::common::config::{ROOT_DATA, RootData};
use bbthings_grpc_server::common::interceptor::interceptor;
use bbthings_grpc_server::common::utility;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use http::{header::HeaderName, Method};
use tower_http::cors::{CorsLayer, Any};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_url: Option<String>,
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    secured: bool
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    let db_url = match args.db_url {
        Some(value) => value,
        None => std::env::var("DATABASE_URL_AUTH").unwrap()
    };
    let address = match args.address {
        Some(value) => value,
        None => std::env::var("BIND_ADDRESS_AUTH").unwrap()
    };
    let secured_env = match std::env::var("SECURED") {
        Ok(value) => ["1", "true", "True", "TRUE"].into_iter().any(|e| *e == value),
        Err(_) => false
    };
    let secured = args.secured || secured_env;

    let root_pw = std::env::var("ROOT_PASSWORD");
    let root_ad = std::env::var("ROOT_ACCESS_DURATION");
    let root_rd = std::env::var("ROOT_REFRESH_DURATION");
    let root_ak = std::env::var("ROOT_ACCESS_KEY");
    if let (Ok(password), Ok(access_duration), Ok(refresh_duration), Ok(root_key)) = (root_pw, root_ad, root_rd, root_ak) {
        ROOT_DATA.set(RootData::new_with_key(
            &password, 
            access_duration.parse()?, 
            refresh_duration.parse()?, 
            &utility::hex_to_bytes(&root_key).unwrap()
        )).unwrap();
    }

    if secured {
        auth_server_secured(db_url, address).await
    } else {
        auth_server(db_url, address).await
    }
}

async fn auth_server(db_url: String, address: String) -> Result<(), Box<dyn std::error::Error>>
{
    let addr = address.parse()?;

    let auth_db = Auth::new_with_url(&db_url).await;
    migrate_auth(&auth_db.pool).await.unwrap();

    let api_server = ApiServer::new(auth_db.clone());
    let role_server = RoleServer::new(auth_db.clone());
    let user_server = UserServer::new(auth_db.clone());
    let profile_server = ProfileServer::new(auth_db.clone());
    let token_server = TokenServer::new(auth_db.clone());
    let auth_server = AuthServer::new(auth_db.clone());

    let api_service = ApiServiceServer::new(api_server);
    let role_service = RoleServiceServer::new(role_server);
    let user_service = UserServiceServer::new(user_server);
    let profile_service = ProfileServiceServer::new(profile_server);
    let token_service = TokenServiceServer::new(token_server);
    let auth_service = AuthServiceServer::new(auth_server);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(descriptor::api::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::role::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::user::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::profile::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::token::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::auth::DESCRIPTOR_SET)
        .build_v1();

    Server::builder()
        .accept_http1(true)
        .layer(CorsLayer::new()
            .allow_origin(Any)
            .allow_headers(Any)
            .allow_methods([Method::POST])
            .expose_headers([HeaderName::from_static("grpc-status"), HeaderName::from_static("grpc-message")])
        )
        .layer(GrpcWebLayer::new())
        .add_service(api_service)
        .add_service(role_service)
        .add_service(user_service)
        .add_service(profile_service)
        .add_service(token_service)
        .add_service(auth_service)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}

async fn auth_server_secured(db_url: String, address: String) -> Result<(), Box<dyn std::error::Error>>
{
    let addr = address.parse()?;

    let auth_db = Auth::new_with_url(&db_url).await;
    migrate_auth(&auth_db.pool).await.unwrap();

    let api_server = ApiServer::new_with_validator(auth_db.clone());
    let role_server = RoleServer::new_with_validator(auth_db.clone());
    let user_server = UserServer::new_with_validator(auth_db.clone());
    let profile_server = ProfileServer::new_with_validator(auth_db.clone());
    let token_server = TokenServer::new_with_validator(auth_db.clone());
    let auth_server = AuthServer::new(auth_db.clone());

    let api_service = ApiServiceServer::with_interceptor(api_server, interceptor);
    let role_service = RoleServiceServer::with_interceptor(role_server, interceptor);
    let user_service = UserServiceServer::with_interceptor(user_server, interceptor);
    let profile_service = ProfileServiceServer::with_interceptor(profile_server, interceptor);
    let token_service = TokenServiceServer::with_interceptor(token_server, interceptor);
    let auth_service = AuthServiceServer::new(auth_server);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(descriptor::api::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::role::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::user::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::profile::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::token::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::auth::DESCRIPTOR_SET)
        .build_v1();

    Server::builder()
        .accept_http1(true)
        .layer(CorsLayer::new()
            .allow_origin(Any)
            .allow_headers(Any)
            .allow_methods([Method::POST])
            .expose_headers([HeaderName::from_static("grpc-status"), HeaderName::from_static("grpc-message")])
        )
        .layer(GrpcWebLayer::new())
        .add_service(api_service)
        .add_service(role_service)
        .add_service(user_service)
        .add_service(profile_service)
        .add_service(token_service)
        .add_service(auth_service)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}
