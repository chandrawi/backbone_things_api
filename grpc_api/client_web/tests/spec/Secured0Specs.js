import { auth, AuthConfig, utility } from '../../bundle.js';

const AUTH_ADDR = "http://localhost:9001";
let authConfig = new AuthConfig(AUTH_ADDR);

const ROOT_NAME = "root";
const ROOT_PASSWORD = "r0ot_P4s5w0rd";
const ROOT_ID = "ffffffff-ffff-ffff-ffff-ffffffffffff";

const API_ID = "00000000-0000-0000-0000-000000000000";

const PROCEDURE_ACCESS = [
    {
        procedure: "read_model",
        roles: ["admin", "user"]
    },
    {
        procedure: "create_model",
        roles: ["admin"]
    },
    {
        procedure: "update_model",
        roles: ["admin"]
    },
    {
        procedure: "delete_model",
        roles: ["admin"]
    }
];
const PROCEDURES = PROCEDURE_ACCESS.map((access) => {
    return {
        id: utility.uuid_v4_hex(),
        name: access.procedure
    };
});
const ROLE_NAMES = PROCEDURE_ACCESS
    .flatMap(item => item.roles)
    .filter((role, index, arr) => arr.indexOf(role) === index);
const ROLES = ROLE_NAMES.map((name) => {
    return {
        id: utility.uuid_v4_hex(),
        name: name
    }
});

const ADMIN_ID = utility.uuid_v4_hex();
const ADMIN_NAME = "administrator";
const ADMIN_PASSWORD = "Adm1n_P4s5w0rd";
const USER_ID = utility.uuid_v4_hex();
const USER_NAME = "username";
const USER_PASSWORD = "Us3r_P4s5w0rd";
const USERS = [
    {
        id: ADMIN_ID,
        name: ADMIN_NAME,
        password: ADMIN_PASSWORD,
        role: "admin"
    },
    {
        id: USER_ID,
        name: USER_NAME,
        password: USER_PASSWORD,
        role: "user"
    }
];


describe("Backbone Things Resource test", function() {

    it("Should login root user", async function() {
        await authConfig.login(ROOT_NAME, ROOT_PASSWORD);
        expect(authConfig.user_id).toEqual(ROOT_ID);
        expect(typeof authConfig.auth_token === 'string').toBeTrue();
    });

    it("Should create api and procedures", async function() {
        const api = await auth.read_api(authConfig, { id: API_ID })
            .catch(() => null);
        if (api === null) {
            const id = await auth.create_api(authConfig, {
                id: API_ID,
                name: "resource api",
                address: "localhost",
                category: "RESOURCE",
                description: "",
                password: "Api_pa55w0rd",
            });
            expect(API_ID).toEqual(id);
        }
        for (const procedure of PROCEDURES) {
            const id = await auth.create_procedure(authConfig, {
                id: procedure.id,
                api_id: API_ID,
                name: procedure.name,
                description: ""
            });
            expect(procedure.id).toEqual(id);
        }
    });

    it("Should create roles and link it to procedures", async function() {
        for (const role of ROLES) {
            const id = await auth.create_role(authConfig, {
                id: role.id,
                api_id: API_ID,
                name: role.name,
                multi: false,
                ip_lock: false,
                access_duration: 3600,
                refresh_duration: 43200
            });
            expect(role.id).toEqual(id);
            for (const access of PROCEDURE_ACCESS) {
                const match = access.roles.some((role_name) => role.name === role_name);
                if (match) {
                    const found = PROCEDURES.find((procedure) => procedure.name == access.procedure);
                    const procedure_id = found ? found.id : null;
                    const add = await auth.add_role_access(authConfig, {
                        id: role.id,
                        procedure_id: procedure_id
                    });
                    expect(add).toBeNull();
                }
            }
        }
    });

    it("Should create user and link it to role", async function() {
        for (const user of USERS) {
            const id = await auth.create_user(authConfig, {
                id: user.id,
                name: user.name,
                email: "",
                phone: "",
                password: user.password
            });
            expect(user.id).toEqual(id);
            const found = ROLES.find((role) => role.name == user.role);
            if (found) {
                const add = await auth.add_user_role(authConfig, {
                    user_id: user.id,
                    role_id: found.id
                });
                expect(add).toBeNull();
            }
        }
    });

    it("Should login then logout regular and administrator user", async function() {
        await authConfig.login(ADMIN_NAME, ADMIN_PASSWORD);
        expect(authConfig.user_id).toEqual(ADMIN_ID);
        expect(typeof authConfig.auth_token === 'string').toBeTrue();
        await authConfig.logout();
        expect(typeof authConfig.auth_token === 'undefined').toBeTrue();
        await authConfig.login(USER_NAME, USER_PASSWORD);
        expect(authConfig.user_id).toEqual(USER_ID);
        expect(typeof authConfig.auth_token === 'string').toBeTrue();
        await authConfig.logout();
        expect(typeof authConfig.auth_token === 'undefined').toBeTrue();
    });

});
