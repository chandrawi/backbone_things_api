use bbthings_database::Resource;
use bbthings_grpc_server::proto::resource::config::config_service_server::ConfigServiceServer;
use bbthings_grpc_server::proto::resource::model::model_service_server::ModelServiceServer;
use bbthings_grpc_server::proto::resource::device::device_service_server::DeviceServiceServer;
use bbthings_grpc_server::proto::resource::group::group_service_server::GroupServiceServer;
use bbthings_grpc_server::proto::resource::set::set_service_server::SetServiceServer;
use bbthings_grpc_server::proto::resource::data::data_service_server::DataServiceServer;
use bbthings_grpc_server::proto::resource::buffer::buffer_service_server::BufferServiceServer;
use bbthings_grpc_server::proto::resource::slice::slice_service_server::SliceServiceServer;
use bbthings_grpc_server::proto::descriptor;
use bbthings_grpc_server::auth::auth::api_login;
use bbthings_grpc_server::resource::config::ConfigServer;
use bbthings_grpc_server::resource::model::ModelServer;
use bbthings_grpc_server::resource::device::DeviceServer;
use bbthings_grpc_server::resource::group::GroupServer;
use bbthings_grpc_server::resource::set::SetServer;
use bbthings_grpc_server::resource::data::DataServer;
use bbthings_grpc_server::resource::buffer::BufferServer;
use bbthings_grpc_server::resource::slice::SliceServer;
use bbthings_grpc_server::common::config::{API_ID, ACCESS_MAP, ROOT_DATA, RootData};
use bbthings_grpc_server::common::validator::AccessSchema;
use bbthings_grpc_server::common::interceptor::interceptor;
use bbthings_grpc_server::common::utility;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use http::{header::HeaderName, Method};
use tower_http::cors::{CorsLayer, Any};
use uuid::Uuid;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_url: Option<String>,
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    secured: bool,
    #[arg(long)]
    auth_address: Option<String>,
    #[arg(long)]
    api_id: Option<String>,
    #[arg(long)]
    password: Option<String>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    let db_url = match args.db_url {
        Some(value) => value,
        None => std::env::var("DATABASE_URL_RESOURCE").unwrap()
    };
    let address = match args.address {
        Some(value) => value,
        None => std::env::var("BIND_ADDRESS_RESOURCE").unwrap()
    };
    let mut auth_address = match args.auth_address {
        Some(value) => value,
        None => std::env::var("SERVER_ADDRESS_AUTH").unwrap()
    };
    let scheme = auth_address.split(":").next().unwrap();
    if !(vec!["http", "https"].contains(&scheme)) {
        auth_address = String::from("http://") + auth_address.as_str();
    }
    let api_id = match args.api_id {
        Some(value) => value,
        None => std::env::var("API_ID").unwrap()
    };
    let password = match args.password {
        Some(value) => value,
        None => std::env::var("API_PASSWORD").unwrap()
    };

    let api_id = Uuid::try_parse(&api_id).unwrap();
    API_ID.set(api_id).unwrap();

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

    if args.secured {
        resource_server_secured(db_url, address, auth_address, api_id, password).await
    } else {
        resource_server(db_url, address).await
    }
}

async fn resource_server(db_url: String, address: String) -> Result<(), Box<dyn std::error::Error>>
{
    let addr = address.parse()?;

    let resource_db = Resource::new_with_url(&db_url).await;
    let config_server = ConfigServer{};
    let model_server = ModelServer::new(resource_db.clone());
    let device_server = DeviceServer::new(resource_db.clone());
    let group_server = GroupServer::new(resource_db.clone());
    let set_server = SetServer::new(resource_db.clone());
    let data_server = DataServer::new(resource_db.clone());
    let buffer_server = BufferServer::new(resource_db.clone());
    let slice_server = SliceServer::new(resource_db.clone());

    let config_service = ConfigServiceServer::new(config_server);
    let model_service = ModelServiceServer::new(model_server);
    let device_service = DeviceServiceServer::new(device_server);
    let group_service = GroupServiceServer::new(group_server);
    let set_service = SetServiceServer::new(set_server);
    let data_service = DataServiceServer::new(data_server);
    let buffer_service = BufferServiceServer::new(buffer_server);
    let slice_service = SliceServiceServer::new(slice_server);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(descriptor::config::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::model::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::device::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::group::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::set::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::data::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::buffer::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::slice::DESCRIPTOR_SET)
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
        .add_service(config_service)
        .add_service(model_service)
        .add_service(device_service)
        .add_service(group_service)
        .add_service(set_service)
        .add_service(data_service)
        .add_service(buffer_service)
        .add_service(slice_service)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}

async fn resource_server_secured(db_url: String, address: String, auth_address: String, api_id: Uuid, password: String) -> Result<(), Box<dyn std::error::Error>> 
{
    let addr = address.parse()?;

    let response = api_login(&auth_address, api_id, &password).await
        .expect("Failed to get api definition from Auth server");
    let token_key = response.access_key;
    let accesses: Vec<AccessSchema> = response.access_procedures
        .into_iter()
        .map(|s| s.into())
        .collect();
    ACCESS_MAP.set(accesses.clone()).unwrap();

    let resource_db = Resource::new_with_url(&db_url).await;
    let config_server = ConfigServer{};
    let model_server = ModelServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let device_server = DeviceServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let group_server = GroupServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let set_server = SetServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let data_server = DataServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let buffer_server = BufferServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let slice_server = SliceServer::new_with_validator(resource_db.clone(), &token_key, &accesses);

    let config_service = ConfigServiceServer::new(config_server);
    let model_service = ModelServiceServer::with_interceptor(model_server, interceptor);
    let device_service = DeviceServiceServer::with_interceptor(device_server, interceptor);
    let group_service = GroupServiceServer::with_interceptor(group_server, interceptor);
    let set_service = SetServiceServer::with_interceptor(set_server, interceptor);
    let data_service = DataServiceServer::with_interceptor(data_server, interceptor);
    let buffer_service = BufferServiceServer::with_interceptor(buffer_server, interceptor);
    let slice_service = SliceServiceServer::with_interceptor(slice_server, interceptor);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(descriptor::config::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::model::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::device::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::group::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::set::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::data::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::buffer::DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(descriptor::slice::DESCRIPTOR_SET)
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
        .add_service(config_service)
        .add_service(model_service)
        .add_service(device_service)
        .add_service(group_service)
        .add_service(set_service)
        .add_service(data_service)
        .add_service(buffer_service)
        .add_service(slice_service)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}
