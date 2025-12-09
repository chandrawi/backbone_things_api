import { auth, utility } from '../../bundle.js';

let server = { address: "http://localhost:9001" };

describe("RMCS Auth test", function() {

    const api_id_1 = utility.uuid_v4_hex();
    const api_id_2 = utility.uuid_v4_hex();
    const api_password_hash = "$argon2id$v=19$m=16,t=2,p=1$ZDFzM09STjM3VW9zYUFveQ$tvA0ovPByPcl5qRDwHvZOg";
    const proc_id_1 = utility.uuid_v4_hex();
    const proc_id_2 = utility.uuid_v4_hex();
    const proc_id_3 = utility.uuid_v4_hex();
    const proc_id_4 = utility.uuid_v4_hex();
    const role_admin_id = utility.uuid_v4_hex();
    const role_user_1_id = utility.uuid_v4_hex();
    const role_user_2_id = utility.uuid_v4_hex();
    const api_name = "Resource_1";
    const proc_name = "ReadData";
    const role_name = "admin";
    const admin_id = utility.uuid_v4_hex();
    const user_id = utility.uuid_v4_hex();
    const admin_password_hash = "$argon2id$v=19$m=16,t=2,p=1$Wnd4RUJXalVVbWtxUXN0Zg$d7Ax5e/o6TRicbCh+koiPg";
    const user_password = "Us3r_P4s5w0rd";
    const user_password_hash = "$argon2id$v=19$m=16,t=2,p=1$aUhFQTQ3aTZxa3JSZXNNQg$ErDWQqiCtfxUM/uUHRf0dQ";
    const new_password_hash = "$argon2id$v=19$m=16,t=2,p=1$MDVycGpVWFZhZ1FLY2dlMQ$tI+2UHr6GP9v8GAizMqfOw";
    let profile_role_id = {};
    let profile_user_id_1 = {};
    let profile_user_id_2 = {};
    let admin_hash = "";
    let userToken = {};

    it("should create API 1", async function() {
        const apiId = await auth.create_api(server, {
            id: api_id_1,
            name: "Resource1",
            address: "localhost:9001",
            category: "RESOURCE",
            description: "",
            password: api_password_hash,
            access_key: utility.random_base64(43)
        });
        expect(apiId.id).toEqual(api_id_1);
    });

    it("should create API 2", async function() {
        const apiId = await auth.create_api(server, {
            id: api_id_2,
            name: "Resource_2",
            address: "localhost:9002",
            category: "RESOURCE",
            description: "",
            password: api_password_hash,
            access_key: utility.random_base64(43)
        });
        expect(apiId.id).toEqual(api_id_2);
    });

    it("should create procedures on API 1", async function() {
        const procId1 = await auth.create_procedure(server, { id: proc_id_1, api_id: api_id_1, name: "ReadResourceData" });
        const procId2 = await auth.create_procedure(server, { id: proc_id_2, api_id: api_id_1, name: "CreateData" });
        const procId3 = await auth.create_procedure(server, { id: proc_id_3, api_id: api_id_1, name: "DeleteData" });
        expect(procId1.id).toEqual(proc_id_1);
        expect(procId2.id).toEqual(proc_id_2);
        expect(procId3.id).toEqual(proc_id_3);
    });

    it("should create procedures on API 2", async function() {
        const proc_id = await auth.create_procedure(server, { id: proc_id_4, api_id: api_id_2, name: "ReadConfig" });
        expect(proc_id.id).toEqual(proc_id_4);
    });

    it("should read multiple API", async function() {
        const apis = await auth.list_api_by_category(server, { category: "RESOURCE" });
        const apiIds = [];
        let api;
        for (const apiSchema of apis) {
            apiIds.push(apiSchema.id);
            if (apiSchema.id == api_id_1) {
                api = apiSchema;
            }
        }
        expect(apiIds).toContain(api_id_2);
        expect(api.name).toEqual("Resource1");
        expect(api.address).toEqual("localhost:9001");
    });

    it("should read procedures", async function() {
        const procs = await auth.list_procedure_by_api(server, { id: api_id_1 });
        const procIds = [];
        for (const procSchema of procs) {
            procIds.push(procSchema.id);
        }
        expect(procIds).toContain(proc_id_1);
    });

    it("should create administrator role and access for API 1", async function() {
        const roleId = await auth.create_role(server, {
            id: role_admin_id,
            api_id: api_id_1,
            name: "administrator",
            multi: false,
            ip_lock: false,
            access_duration: 900,
            refresh_duration: 28800
        });
        await auth.add_role_access(server, { id: role_admin_id, procedure_id: proc_id_1 });
        await auth.add_role_access(server, { id: role_admin_id, procedure_id: proc_id_2 });
        await auth.add_role_access(server, { id: role_admin_id, procedure_id: proc_id_3 });
        expect(roleId.id).toEqual(role_admin_id);
    });

    it("should create user role and access for API 1", async function() {
        const roleId = await auth.create_role(server, {
            id: role_user_1_id,
            api_id: api_id_1,
            name: "user",
            multi: true,
            ip_lock: false,
            access_duration: 900,
            refresh_duration: 604800
        });
        await auth.add_role_access(server, { id: role_user_1_id, procedure_id: proc_id_1 });
        expect(roleId.id).toEqual(role_user_1_id);
    });

    it("should create user role and access for API 2", async function() {
        const roleId = await auth.create_role(server, {
            id: role_user_2_id,
            api_id: api_id_2,
            name: "user",
            multi: true,
            ip_lock: false,
            access_duration: 900,
            refresh_duration: 604800
        });
        await auth.add_role_access(server, { id: role_user_2_id, procedure_id: proc_id_4 });
        expect(roleId.id).toEqual(role_user_2_id);
    });

    it("should read roles on API 1", async function() {
        const roles = await auth.list_role_by_api(server, { id: api_id_1 });
        const role_ids = [];
        let role;
        for (const roleSchema of roles) {
            role_ids.push(roleSchema.id);
            if (roleSchema.id == role_admin_id) {
                role = roleSchema;
            }
        }
        expect(role_ids).toContain(role_user_1_id);
        expect(role.name).toEqual("administrator");
        expect(role.multi).toEqual(false);
        expect(role.procedures).toContain(proc_id_1);
        expect(role.procedures).toContain(proc_id_2);
        expect(role.procedures).toContain(proc_id_3);
    });

    it("should update API 1", async function() {
        const apiUpdate = await auth.update_api(server, { id: api_id_1, name: api_name, description: "New resource api" });
        expect(apiUpdate).toEqual({});
        const api = await auth.read_api_by_name(server, { name: api_name });
        expect(api.id).toEqual(api_id_1);
        expect(api.description).toEqual("New resource api");
    });

    it("should update a procedure", async function() {
        const procUpdate = await auth.update_procedure(server, { id: proc_id_1, name: proc_name, description: "Read resource data" });
        expect(procUpdate).toEqual({});
        const proc = await auth.read_procedure_by_name(server, { api_id: api_id_1, name: proc_name });
        expect(proc.description).toEqual("Read resource data");
    });

    it("should update administrator role", async function() {
        const roleUpdate = await auth.update_role(server, { id: role_admin_id, name: role_name, ip_lock: true });
        expect(roleUpdate).toEqual({});
        const role = await auth.read_role_by_name(server, { api_id: api_id_1, name: role_name });
        expect(role.ip_lock).toEqual(true);
    });

    it("should create administrator user and add associated roles", async function() {
        const userId = await auth.create_user(server, {
            id: admin_id,
            name: "administrator",
            email: "admin@mail.co",
            phone: "+6281234567890",
            password: admin_password_hash
        });
        await auth.add_user_role(server, { user_id: admin_id, role_id: role_admin_id });
        await auth.add_user_role(server, { user_id: admin_id, role_id: role_user_2_id });
        expect(userId.id).toEqual(admin_id);
    });

    it("should create regular user and add associated roles", async function() {
        const userId = await auth.create_user(server, {
            id: user_id,
            name: "username",
            email: "user@mail.co",
            phone: "+6281234567890",
            password: user_password_hash
        });
        await auth.add_user_role(server, { user_id: user_id, role_id: role_user_1_id });
        await auth.add_user_role(server, { user_id: user_id, role_id: role_user_2_id });
        expect(userId.id).toEqual(user_id);
    });

    it("should read users of user 2 role", async function() {
        const users = await auth.list_user_by_role(server, { id: role_user_2_id });
        const userIds = [];
        let user;
        for (const userSchema of users) {
            userIds.push(userSchema.id);
            if (userSchema.id == admin_id) {
                user = userSchema;
            }
        }
        expect(userIds).toContain(user_id);
        expect(user.name).toEqual("administrator");
        expect(user.email).toEqual("admin@mail.co");
        admin_hash = user.password;
    });

    it("should update administrator user", async function() {
        const userUpdate = await auth.update_user(server, { id: admin_id, password: new_password_hash });
        expect(userUpdate).toEqual({});
        const user = await auth.read_user_by_name(server, { name: "administrator" });
        expect(user.password).not.toEqual(admin_hash);
    });

    it("should create role and user profile", async function() {
        profile_role_id = await auth.create_role_profile(server, {
            role_id: role_admin_id,
            name: "name",
            value_type: "STRING",
            mode: "SINGLE_REQUIRED"
        });
        profile_user_id_1 = await auth.create_user_profile(server, {
            user_id: admin_id,
            name: "name",
            value: "john doe"
        });
        const profile_role = await auth.read_role_profile(server, { id: profile_role_id.id});
        expect(profile_role.name).toEqual("name");
        expect(profile_role.mode).toEqual("SINGLE_REQUIRED");
        const profile_user = await auth.read_user_profile(server, { id: profile_user_id_1.id });
        expect(profile_user.value).toEqual("john doe");
        profile_user_id_2 = await auth.create_user_profile(server, {
            user_id: admin_id,
            name: "age",
            value: 20
        });
    });

    it("should update user profile", async function() {
        const userProfileUpdate = await auth.update_user_profile(server, { id: profile_user_id_2.id, value: 21 });
        expect(userProfileUpdate).toEqual({});
        const profile_user = await auth.read_user_profile(server, { id: profile_user_id_2.id });
        expect(profile_user.value).toEqual(21);
    });

    const expire_1 = new Date(2023, 0, 1, 0, 0, 0);
    const expire_2 = new Date(2023, 0, 1, 12, 0, 0);
    const expire_3 = new Date(2023, 0, 1, 18, 0, 0);
    const auth_token = "rGKrHrDuWXt2CDbjmrt1SHbmea86wIQb";
    const ip = [192, 168, 0, 1];
    let access_id_1;
    let access_id_2;
    let access_id_3;

    it("should create access tokens", async function() {
        const token1 = await auth.create_access_token(server, { user_id: admin_id, auth_token: auth_token, expired: expire_1, ip: ip });
        access_id_1 = token1.access_id;
        expect(token1.auth_token).toEqual(auth_token);
        const token2 = await auth.create_access_token(server, { user_id: admin_id, auth_token: auth_token, expired: expire_1, ip: "" });
        access_id_2 = token2.access_id;
        expect(access_id_2).toEqual(access_id_1 + 1);
    });

    it("should create an auth token", async function() {
        const tokens = await auth.create_auth_token(server, { user_id: admin_id, expired: expire_2, ip: ip, number: 1 });
        access_id_3 = tokens[0].access_id;
        expect(tokens[0].access_id).toEqual(access_id_2 + 1);
    });

    it("should read an access token", async function() {
        const token = await auth.read_access_token(server, { access_id: access_id_2 });
        expect(token.expired).toEqual(expire_1);
    });

    it("should read auth tokens", async function() {
        const tokens = await auth.list_auth_token(server, { auth_token: auth_token });
        let token;
        for (const tokenSchema of tokens) {
            if (tokenSchema.access_id == access_id_1) {
                token = tokenSchema;
            }
        }
        expect(token.user_id).toEqual(admin_id);
        expect(token.expired).toEqual(expire_1);
        expect(token.ip).toEqual(ip);
    });

    it("should read tokens associated with user", async function() {
        const tokens = await auth.list_token_by_user(server, { id: admin_id });
        expect(tokens.length).toEqual(3);
    });

    it("should update an access token", async function() {
        await auth.update_access_token(server, { access_id: access_id_2, expired: expire_3 });
        const token = await auth.read_access_token(server, { access_id: access_id_2 });
        expect(token.expired).toEqual(expire_3);
    });

    it("should update auth tokens", async function() {
        await auth.update_auth_token(server, { auth_token: auth_token, expired: expire_3, ip: [192, 168, 0, 100]});
        const tokens = await auth.list_auth_token(server, { auth_token: auth_token });
        expect(tokens[0].expired).toEqual(expire_3);
        expect(tokens[0].ip).toEqual([192, 168, 0, 100]);
    });

    it("should login a user", async function() {
        const login = await auth.user_login(server, {
            username: "username",
            password: user_password
        });
        expect(login.user_id).toEqual(user_id);
        let api_ids = [];
        for (const tokenMap of login.access_tokens) {
            api_ids.push(tokenMap.api_id);
            if (tokenMap.api_id == api_id_1) {
                userToken = {
                    api_id: tokenMap.api_id,
                    access_token: tokenMap.access_token,
                    refresh_token: tokenMap.refresh_token,
                    auth_token: login.auth_token,
                    user_id: login.user_id
                };
            }
        }
        expect(api_ids).toContain(api_id_1);
        expect(api_ids).toContain(api_id_2);
        const tokens = await auth.list_token_by_user(server, { id: user_id });
        expect(tokens.length).toEqual(2);
    });

    it("should refresh a user", function(done) {
        setTimeout(function() {
            auth.user_refresh(server, {
                api_id: userToken.api_id,
                access_token: userToken.access_token,
                refresh_token: userToken.refresh_token
            }).then((refresh) => {
                expect(refresh.access_token).not.toEqual(userToken.access_token);
                expect(refresh.refresh_token).not.toEqual(userToken.refresh_token);
                done();
            });
        }, 1000); // make sure refresh requested at least 1 second later after login
    });

    it("should logout a user", async function() {
        const logout = await auth.user_logout(server, {
            user_id: userToken.user_id,
            auth_token: userToken.auth_token
        });
        expect(logout).toEqual({});
        const tokens = await auth.list_auth_token(server, { auth_token: userToken.auth_token });
        expect(tokens.length).toEqual(0);
    });

    it("should delete role and user profile", async function() {
        const ProfileRoleDelete = await auth.delete_role_profile(server, { id: profile_role_id })
            .catch(() => true);
        expect(ProfileRoleDelete).toBeTrue();
        const ProfileUserDelete = await auth.delete_user_profile(server, { id: profile_user_id_1 })
            .catch(() => true);
        expect(ProfileUserDelete).toBeTrue();
    });

    it("should failed to delete a role", async function() {
        const roleDelete = await auth.delete_role(server, { id: role_user_2_id })
            .catch(() => true);
        expect(roleDelete).toBeTrue();
    });

    it("should failed to delete an API", async function() {
        const apiDelete = await auth.delete_api(server, { id: api_id_2 })
            .catch(() => null);
        expect(apiDelete).toBeNull();
    });

    it("should failed to delete a user", async function() {
        const userDelete = await auth.delete_user(server, { id: user_id })
            .catch(() => null);
        expect(userDelete).toBeNull();
    });

    it("should delete tokens", async function() {
        const tokenDelete = await auth.delete_token_by_user(server, { id: admin_id });
        expect(tokenDelete).toEqual({});
        const token = await auth.read_access_token(server, { access_id: access_id_1 })
            .catch(() => null);
        expect(token).toBeNull();
    });

    it("should delete regular user", async function() {
        await auth.remove_user_role(server, { user_id: user_id, role_id: role_user_1_id });
        await auth.remove_user_role(server, { user_id: user_id, role_id: role_user_2_id });
        const userDelete = await auth.delete_user(server, { id: user_id });
        expect(userDelete).toEqual({});
        const user = await auth.read_user(server, { id: user_id })
            .catch(() => null);
        expect(user).toBeNull();
    });

    it("should delete user role", async function() {
        await auth.remove_user_role(server, { user_id: admin_id, role_id: role_user_2_id });
        await auth.remove_role_access(server, { id: role_user_2_id, procedure_id: proc_id_4 });
        const roleDelete = await auth.delete_role(server, { id: role_user_2_id });
        expect(roleDelete).toEqual({});
        const role = await auth.read_role(server, { id: role_user_2_id })
            .catch(() => null);
        expect(role).toBeNull();
    });

    it("should delete a procedure", async function() {
        const procDelete = await auth.delete_procedure(server, { id: proc_id_4 });
        expect(procDelete).toEqual({});
        const proc = await auth.read_procedure(server, { id: proc_id_4 })
            .catch(() => null);
        expect(proc).toBeNull();
    });

    it("should delete API 2", async function() {
        const apiDelete = await auth.delete_api(server, { id: api_id_2 });
        expect(apiDelete).toEqual({});
        const api = await auth.read_api(server, { id: api_id_2 })
            .catch(() => null);
        expect(api).toBeNull();
    });

});
