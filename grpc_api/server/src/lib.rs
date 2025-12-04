pub mod auth {
    pub mod api;
    pub mod role;
    pub mod user;
    pub mod profile;
    pub mod token;
}
pub mod resource {
    pub mod model;
    pub mod device;
    pub mod group;
    pub mod set;
    pub mod data;
    pub mod buffer;
    pub mod slice;
}
pub mod common {
    pub mod utility;
    pub mod config;
    pub mod token;
    pub mod validator;
    pub mod interceptor;
}
