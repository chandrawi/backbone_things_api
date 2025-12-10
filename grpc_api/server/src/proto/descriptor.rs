pub mod api {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("api_descriptor");
}

pub mod role {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("role_descriptor");
}

pub mod user {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("user_descriptor");
}

pub mod profile {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("profile_descriptor");
}

pub mod token {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("token_descriptor");
}

pub mod auth {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("auth_descriptor");
}


pub mod config {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("config_descriptor");
}

pub mod model {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("model_descriptor");
}

pub mod device {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("device_descriptor");
}

pub mod group {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("group_descriptor");
}

pub mod set {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("set_descriptor");
}

pub mod data {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("data_descriptor");
}

pub mod buffer {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("buffer_descriptor");
}

pub mod slice {
    pub const DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("slice_descriptor");
}
