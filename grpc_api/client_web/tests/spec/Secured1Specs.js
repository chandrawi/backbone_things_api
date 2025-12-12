import { resource, ResourceConfig, utility, DataType } from '../../bundle.js';

const AUTH_ADDR = "http://localhost:9001";
const RESOURCE_ADDR = "http://localhost:9002";
let adminConfig = new ResourceConfig(RESOURCE_ADDR);
let userConfig = new ResourceConfig(RESOURCE_ADDR);

const ADMIN_NAME = "administrator";
const ADMIN_PASSWORD = "Adm1n_P4s5w0rd";
const USER_NAME = "username";
const USER_PASSWORD = "Us3r_P4s5w0rd";

const MODEL_ID = utility.uuid_v4_hex();

describe("Backbone Things Resource test", function() {

    it("Should login admin and regular user", async function() {
        await adminConfig.login(AUTH_ADDR, ADMIN_NAME, ADMIN_PASSWORD);
        expect(adminConfig.user_id).toBeDefined();
        expect(adminConfig.auth_token).toBeDefined();
        await userConfig.login(AUTH_ADDR, USER_NAME, USER_PASSWORD);
        expect(userConfig.user_id).toBeDefined();
        expect(userConfig.auth_token).toBeDefined();
    });

    it("Should failed to create a model by regular user", async function() {
        const model_id = await resource.create_model(userConfig, {
            id: MODEL_ID,
            category: "UPLINK",
            name: "name",
            description: "",
            data_type: [DataType.F32, DataType.F64]
        }).catch(() => true);
        expect(model_id).toBeTrue();
    });

    it("Should success to create a model by administrator user", async function() {
        const model_id = await resource.create_model(adminConfig, {
            id: MODEL_ID,
            category: "UPLINK",
            name: "name",
            description: "",
            data_type: [DataType.F32, DataType.F64]
        });
        expect(model_id).toEqual(MODEL_ID);
    });

    it("Should read created model by regular user", async function() {
        const model = await resource.read_model(userConfig, {
            id: MODEL_ID
        });
        expect(model.id).toEqual(MODEL_ID);
    });

    it("Should refresh regular user token", async function() {
        const old_token = userConfig.refresh_token;
        await userConfig.refresh();
        expect(old_token).not.toEqual(userConfig.refresh_token);
        const model = await resource.read_model(userConfig, {
            id: MODEL_ID
        });
        expect(model.id).toEqual(MODEL_ID);
    });

    it("Should logout admin and regular user", async function() {
        await adminConfig.logout();
        expect(adminConfig.auth_token).not.toBeDefined();
        await userConfig.logout();
        expect(userConfig.auth_token).not.toBeDefined();
    });

    it("Should failed to access resource after logout", async function() {
        const model = await resource.read_model(userConfig, {
            id: MODEL_ID
        })
        .catch(() => true);
        expect(model).toBeTrue();
        const update = await resource.read_model(adminConfig, {
            id: MODEL_ID
        })
        .catch(() => true);
        expect(update).toBeTrue();
    });

});
