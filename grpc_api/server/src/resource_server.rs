use bbthings_database::Resource;
use bbthings_grpc_proto::auth::auth::auth_service_client::AuthServiceClient;
use bbthings_grpc_proto::auth::auth::{ApiKeyRequest, ApiLoginRequest, ApiLoginResponse};
use bbthings_grpc_proto::resource::model::model_service_server::ModelServiceServer;
use bbthings_grpc_proto::resource::device::device_service_server::DeviceServiceServer;
use bbthings_grpc_proto::resource::group::group_service_server::GroupServiceServer;
use bbthings_grpc_proto::resource::set::set_service_server::SetServiceServer;
use bbthings_grpc_proto::resource::data::data_service_server::DataServiceServer;
use bbthings_grpc_proto::resource::buffer::buffer_service_server::BufferServiceServer;
use bbthings_grpc_proto::resource::slice::slice_service_server::SliceServiceServer;
use bbthings_grpc_proto::descriptor;
use bbthings_grpc_server::resource::model::ModelServer;
use bbthings_grpc_server::resource::device::DeviceServer;
use bbthings_grpc_server::resource::group::GroupServer;
use bbthings_grpc_server::resource::set::SetServer;
use bbthings_grpc_server::resource::data::DataServer;
use bbthings_grpc_server::resource::buffer::BufferServer;
use bbthings_grpc_server::resource::slice::SliceServer;
use bbthings_grpc_server::common::config::{ROOT_DATA, RootData};
use bbthings_grpc_server::common::validator::AccessSchema;
use bbthings_grpc_server::common::interceptor::interceptor;
use bbthings_grpc_server::common::utility;
use tonic::{Request, transport::Channel};
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
    let model_server = ModelServer::new(resource_db.clone());
    let device_server = DeviceServer::new(resource_db.clone());
    let group_server = GroupServer::new(resource_db.clone());
    let set_server = SetServer::new(resource_db.clone());
    let data_server = DataServer::new(resource_db.clone());
    let buffer_server = BufferServer::new(resource_db.clone());
    let slice_server = SliceServer::new(resource_db.clone());

    let model_server = ModelServiceServer::new(model_server);
    let device_server = DeviceServiceServer::new(device_server);
    let group_server = GroupServiceServer::new(group_server);
    let set_server = SetServiceServer::new(set_server);
    let data_server = DataServiceServer::new(data_server);
    let buffer_server = BufferServiceServer::new(buffer_server);
    let slice_server = SliceServiceServer::new(slice_server);

    let reflection_service = tonic_reflection::server::Builder::configure()
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
        .add_service(model_server)
        .add_service(device_server)
        .add_service(group_server)
        .add_service(set_server)
        .add_service(data_server)
        .add_service(buffer_server)
        .add_service(slice_server)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}

async fn resource_server_secured(db_url: String, address: String, auth_address: String, api_id: String, password: String) -> Result<(), Box<dyn std::error::Error>> 
{
    let addr = address.parse()?;
    let api_id = Uuid::try_parse(&api_id).unwrap();

    let response = api_login(&auth_address, api_id, &password).await
        .expect("Failed to get api definition from Auth server");
    let token_key = response.access_key;
    let accesses: Vec<AccessSchema> = response.access_procedures
        .into_iter()
        .map(|s| s.into())
        .collect();

    let resource_db = Resource::new_with_url(&db_url).await;
    let model_server = ModelServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let device_server = DeviceServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let group_server = GroupServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let set_server = SetServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let data_server = DataServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let buffer_server = BufferServer::new_with_validator(resource_db.clone(), &token_key, &accesses);
    let slice_server = SliceServer::new_with_validator(resource_db.clone(), &token_key, &accesses);

    let model_server = ModelServiceServer::with_interceptor(model_server, interceptor);
    let device_server = DeviceServiceServer::with_interceptor(device_server, interceptor);
    let group_server = GroupServiceServer::with_interceptor(group_server, interceptor);
    let set_server = SetServiceServer::with_interceptor(set_server, interceptor);
    let data_server = DataServiceServer::with_interceptor(data_server, interceptor);
    let buffer_server = BufferServiceServer::with_interceptor(buffer_server, interceptor);
    let slice_server = SliceServiceServer::with_interceptor(slice_server, interceptor);

    let reflection_service = tonic_reflection::server::Builder::configure()
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
        .add_service(model_server)
        .add_service(device_server)
        .add_service(group_server)
        .add_service(set_server)
        .add_service(data_server)
        .add_service(buffer_server)
        .add_service(slice_server)
        .add_service(reflection_service?)
        .serve(addr)
        .await?;

    Ok(())
}

pub async fn api_login(addr: &str, api_id: Uuid, password: &str)
    -> Option<ApiLoginResponse>
{
    let channel = Channel::from_shared(addr.to_owned())
        .expect("Invalid address")
        .connect()
        .await
        .expect(&format!("Error making channel to {}", addr));
    let mut client = AuthServiceClient::new(channel.to_owned());
    let request = Request::new(ApiKeyRequest {
    });
    // get transport public key of requested API and encrypt the password
    let response = client.api_login_key(request).await.ok()?.into_inner();
    let pub_key = utility::import_public_key(response.public_key.as_slice()).ok()?;
    let passhash = utility::encrypt_message(password.as_bytes(), pub_key).ok()?;
    // request API key and procedures access from server
    let (priv_key, pub_key) = utility::generate_transport_keys().ok()?;
    let pub_der = utility::export_public_key(pub_key).ok()?;
    let request = Request::new(ApiLoginRequest {
        api_id: api_id.as_bytes().to_vec(),
        password: passhash,
        public_key: pub_der
    });
    let mut response = client.api_login(request).await.ok()?.into_inner();
    response.access_key = utility::decrypt_message(&response.access_key, priv_key).ok()?;
    Some(response)
}
