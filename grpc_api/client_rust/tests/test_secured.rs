#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use bbthings_grpc_client::{Auth, Resource, utility, DataType};
    use bbthings_grpc_server::common::test::{TestServerKind, TestServer};
    use bbthings_grpc_server::common::config::{ROOT_ID, ROOT_NAME};
    const ROOT_PASSWORD: &str = "r0ot_P4s5w0rd";

    #[tokio::test]
    async fn test_resource()
    {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1"); }

        let api_id = Uuid::new_v4();
        let api_password = "Api_pa55w0rd";

        let procedure_access: &[(&str, &[&str])] = &[
            ("read_model", &["admin", "user"]),
            ("create_model", &["admin"]),
            ("update_model", &["admin"]),
            ("delete_model", &["admin"])
        ];
        let procedures: Vec<(Uuid, &str)> = procedure_access.iter()
            .map(|(procedure, _)| {
                (Uuid::new_v4(), *procedure)
            }).collect();
        let roles: Vec<(Uuid, &str)> = procedure_access.iter()
            .flat_map(|(_, roles)| *roles)
            .fold(Vec::new(), |mut acc, role| {
                if !acc.contains(&role) {
                    acc.push(role);
                }
                acc
            }).into_iter()
            .map(|r| (Uuid::new_v4(), *r))
            .collect();
        let mut role_access = Vec::new();

        let admin_id = Uuid::new_v4();
        let admin_name = "admin";
        let admin_password = "Adm1n_P4s5w0rd";
        let user_id = Uuid::new_v4();
        let user_name = "username";
        let user_password = "Us3r_P4s5w0rd";
        let users = &[
            (admin_id, admin_name, admin_password, "admin"),
            (user_id, user_name, user_password, "user")
        ];
        let mut user_role = Vec::new();

        // start auth server
        let auth_server = TestServer::new(TestServerKind::Auth);
        auth_server.truncate_tables().await.unwrap();
        auth_server.start_server();

        // root login
        let mut auth_root = Auth::new(&auth_server.address).await;
        auth_root.login(ROOT_NAME, ROOT_PASSWORD).await.unwrap();
        assert_eq!(auth_root.user_id, Some(ROOT_ID));

        // create api and procedures
        let access_key = utility::generate_access_key();
        let id = auth_root.create_api(api_id, "resource api", "localhost", "RESOURCE", "", api_password, &access_key)
            .await.unwrap();
        assert_eq!(id, api_id);
        for (proc_id, proc_name) in &procedures {
            let id = auth_root.create_procedure(*proc_id, api_id, proc_name, "")
                .await.unwrap();
            assert_eq!(id, *proc_id);
        }

        // create roles and link it to procedures
        for (role_id, role_name) in &roles {
            let id = auth_root.create_role(*role_id, api_id, role_name, false, false, 3600, 43200)
                .await.unwrap();
            assert_eq!(id, *role_id);
            for (procedure_name, role_names) in procedure_access {
                if role_names.iter().any(|r| r == role_name) {
                    let (procedure_id, _) = procedures.iter().find(|(_, p)| p == procedure_name).unwrap();
                    auth_root.add_role_access(*role_id, *procedure_id)
                        .await.unwrap();
                    role_access.push((*role_id, *procedure_id));
                }
            }
        }

        // create users and link it to a role
        for (user_id, name, password, role) in users {
            let id = auth_root.create_user(*user_id, name, "", "", password)
                .await.unwrap();
            assert_eq!(id, *user_id);
            let (role_id, _) = roles.iter().find(|(_, r)| r == role).unwrap();
            auth_root.add_user_role(*user_id, *role_id)
                .await.unwrap();
            user_role.push((*user_id, *role_id));
        }

        // test newly created admin user and regular user to login to Auth server and then logout
        let mut auth_admin = Auth::new(&auth_server.address).await;
        auth_admin.login(admin_name, admin_password).await.unwrap();
        assert_eq!(auth_admin.user_id, Some(admin_id));
        auth_admin.logout().await.unwrap();
        assert_eq!(auth_admin.user_id, None);
        let mut auth_user = Auth::new(&auth_server.address).await;
        auth_user.login(user_name, user_password).await.unwrap();
        assert_eq!(auth_user.user_id, Some(user_id));
        auth_user.logout().await.unwrap();
        assert_eq!(auth_user.user_id, None);

        // start resource server for testing
        let resource_server = TestServer::new_secured(TestServerKind::Resource, Some(&api_id.to_string()), Some(api_password));
        resource_server.start_server();

        // admin and regular user login
        let mut resource_admin = Resource::new(&resource_server.address).await;
        resource_admin.login(&auth_server.address, admin_name, admin_password).await.unwrap();
        assert_eq!(resource_admin.api_id, Some(api_id));
        assert_eq!(resource_admin.user_id, Some(admin_id));
        let mut resource_user = Resource::new(&resource_server.address).await;
        resource_user.login(&auth_server.address, user_name, user_password).await.unwrap();
        assert_eq!(resource_user.api_id, Some(api_id));
        assert_eq!(resource_user.user_id, Some(user_id));

        // try to create model by regular user and admin user, regular user should failed and admin user should success
        let try_create = resource_user.create_model(Uuid::new_v4(), &[DataType::F32T, DataType::F64T], "UPLINK", "name", None)
            .await;
        assert!(try_create.is_err());
        let model_id = resource_admin.create_model(Uuid::new_v4(), &[DataType::F32T, DataType::F64T], "UPLINK", "name", None)
            .await.unwrap();

        // read created model using user service
        let model = resource_user.read_model(model_id).await.unwrap();
        assert_eq!(model.category, "UPLINK");
        assert_eq!(model.name, "name");

        // refresh regular user and then try to read model again after refreshing token
        resource_user.refresh().await.unwrap();
        resource_user.read_model(model_id).await.unwrap();

        // regular user and admin user logout
        resource_admin.logout().await.unwrap();
        assert_eq!(resource_admin.user_id, None);
        resource_user.logout().await.unwrap();
        assert_eq!(resource_user.user_id, None);

        // try to access resource after logout, regular and admin user should failed
        let try_user = resource_user.read_model(model_id).await;
        let try_admin = resource_admin.read_model(model_id).await;
        assert!(try_user.is_err());
        assert!(try_admin.is_err());

        // remove user links to role and delete user
        for (user_id, role_id) in user_role {
            auth_root.remove_user_role(user_id, role_id).await.unwrap();
            auth_root.delete_user(user_id).await.unwrap();
        }

        // remove role links to procedure and delete roles
        for (role_id, procedure_id) in role_access {
            auth_root.remove_role_access(role_id, procedure_id).await.unwrap();
        }
        for (role_id, _) in roles {
            auth_root.delete_role(role_id).await.unwrap();
        }

        // delete procedures and api
        for (procedure_id, _) in procedures {
            auth_root.delete_procedure(procedure_id).await.unwrap();
        }
        auth_root.delete_api(api_id).await.unwrap();

        // root logout
        auth_root.logout().await.unwrap();
        assert_eq!(auth_root.user_id, None);

        // try to read api after logout, should error
        let try_read = auth_root.read_api(api_id).await;
        assert!(try_read.is_err());

        // stop auth and resource server
        resource_server.stop_server();
        auth_server.stop_server();
    }

}
