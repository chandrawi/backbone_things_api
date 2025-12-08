use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let proto_files = [
        ("../proto/bbthings_grpc/proto/auth/api.proto", "api_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/auth/role.proto", "role_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/auth/user.proto", "user_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/auth/profile.proto", "profile_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/auth/token.proto", "token_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/auth/auth.proto", "auth_descriptor.bin")
    ];

    for tuple in proto_files {
        let (fproto, fdescriptor) = tuple;
        tonic_prost_build::configure()
            .protoc_arg("--experimental_allow_proto3_optional") // for older systems
            .build_server(true)
            .build_client(true)
            .file_descriptor_set_path(out_dir.join(fdescriptor))
            .out_dir("./src/proto/auth")
            .compile_protos(&[fproto], &["../proto/bbthings_grpc/proto/auth"])?;
    }

    let proto_files = [
        ("../proto/bbthings_grpc/proto/resource/model.proto", "model_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/device.proto", "device_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/group.proto", "group_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/set.proto", "set_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/data.proto", "data_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/buffer.proto", "buffer_descriptor.bin"),
        ("../proto/bbthings_grpc/proto/resource/slice.proto", "slice_descriptor.bin")
    ];

    for tuple in proto_files {
        let (fproto, fdescriptor) = tuple;
        tonic_prost_build::configure()
            .protoc_arg("--experimental_allow_proto3_optional") // for older systems
            .build_server(true)
            .build_client(true)
            .file_descriptor_set_path(out_dir.join(fdescriptor))
            .out_dir("./src/proto/resource")
            .compile_protos(&[fproto], &["../proto/bbthings_grpc/proto/resource"])?;
    }

    Ok(())
}
