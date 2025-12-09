import { get_data_value, set_data_value, get_data_type, set_data_type } from '../common/type_value.js';
import pb_profile from "../proto/auth/profile_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64
} from "../common/utility.js";


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * @typedef {Object} ServerConfig
 * @property {string} address
 * @property {?string} token
 */

/**
 * @typedef {Object} ProfileId
 * @property {number} id
 */

/**
 * @typedef {Object} RoleId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} UserId
 * @property {Uuid} id
 */

/**
 * @param {*} r 
 * @returns {ProfileId}
 */
function get_profile_id(r) {
    return {
        id: r.id
    };
}

/**
 * @typedef {Object} RoleProfileSchema
 * @property {number} id
 * @property {Uuid} role_id
 * @property {string} name
 * @property {number|string} value_type
 * @property {number|string} mode
 */

/**
 * @param {*} r 
 * @returns {RoleProfileSchema}
 */
function get_role_profile_schema(r) {
    return {
        id: r.id,
        role_id: base64_to_uuid_hex(r.roleId),
        name: r.name,
        value_type: get_data_type(r.valueType),
        mode: get_profile_mode(r.mode)
    };
}

/**
 * @param {*} r 
 * @returns {RoleProfileSchema[]}
 */
function get_role_profile_schema_vec(r) {
    return r.map((v) => {return get_role_profile_schema(v)});
}

/**
 * @typedef {Object} UserProfileSchema
 * @property {number} id
 * @property {Uuid} user_id
 * @property {string} name
 * @property {number|bigint|string|Uint8Array|boolean} value
 * @property {number} order
 */

/**
 * @param {*} r 
 * @returns {UserProfileSchema}
 */
function get_user_profile_schema(r) {
    return {
        id: r.id,
        user_id: base64_to_uuid_hex(r.userId),
        name: r.name,
        value: get_data_value(r.valueBytes, r.valueType),
        order: r.order
    };
}

/**
 * @param {*} r 
 * @returns {UserProfileSchema[]}
 */
function get_user_profile_schema_vec(r) {
    return r.map((v) => {return get_user_profile_schema(v)});
}

/**
 * @typedef {Object} RoleProfileUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|string} value_type
 * @property {?number|string} mode
 */

/**
 * @typedef {Object} UserProfileUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|bigint|string|Uint8Array|boolean} value
 */

/**
 * @typedef {Object} UserProfileSwap
 * @property {Uuid} user_id
 * @property {string} name
 * @property {number} order_1
 * @property {number} order_2
 */

/**
 * @param {number} mode 
 * @returns {string}
 */
function get_profile_mode(mode) {
    switch (mode) {
        case 1: return "SINGLE_REQUIRED";
        case 2: return "MULTIPLE_OPTIONAL";
        case 3: return "MULTIPLE_REQUIRED";
        default: return "SINGLE_OPTIONAL";
    }
}

/**
 * @param {?number|string} mode 
 * @returns {number}
 */
function set_profile_mode(mode) {
    if (typeof mode == "number") {
        if (mode > 0 && mode <= 3) return mode;
    }
    if (typeof mode == "string") {
        mode = mode.replace(/[a-z][A-Z]/, s => `${s.charAt(0)}_${s.charAt(1)}`);
        switch (mode.toUpperCase()) {
            case "SINGLE_OPTIONAL": return 0;
            case "SINGLE_REQUIRED": return 1;
            case "MULTIPLE_OPTIONAL": return 2;
            case "MULTIPLE_REQUIRED": return 3;
        }
    }
    return 0;
}


/**
 * Read a role profile by id
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<RoleProfileSchema>} role profile schema: id, role_id, name, value_type, mode
 */
export async function read_role_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.readRoleProfile(profileId, metadata(server))
        .then(response => get_role_profile_schema(response.toObject().result));
}

/**
 * Read role profiles by role id
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleId} request role id: id
 * @returns {Promise<RoleProfileSchema[]>} role profile schema: id, role_id, name, value_type, mode
 */
export async function list_role_profile_by_role(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const roleId = new pb_profile.RoleId();
    roleId.setId(uuid_hex_to_base64(request.id));
    return client.listRoleProfile(roleId, metadata(server))
        .then(response => get_role_profile_schema_vec(response.toObject().resultsList));
}

/**
 * Create a role profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleProfileSchema} request role profile schema: role_id, name, value_type, mode
 * @returns {Promise<ProfileId>} profile id: id
 */
export async function create_role_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const roleProfileSchema = new pb_profile.RoleProfileSchema();
    roleProfileSchema.setRoleId(uuid_hex_to_base64(request.role_id));
    roleProfileSchema.setName(request.name);
    roleProfileSchema.setValueType(set_data_type(request.value_type));
    roleProfileSchema.setMode(set_profile_mode(request.mode));
    return client.createRoleProfile(roleProfileSchema, metadata(server))
        .then(response => get_profile_id(response.toObject()));
}

/**
 * Update a role profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleProfileUpdate} request role update: id, name, value_type, mode
 * @returns {Promise<{}>} update response
 */
export async function update_role_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const roleProfileUpdate = new pb_profile.RoleProfileUpdate();
    roleProfileUpdate.setId(request.id);
    roleProfileUpdate.setName(request.name);
    if (request.value_type) {
        roleProfileUpdate.setValueType(set_data_type(request.value_type));
    }
    roleProfileUpdate.setMode(set_profile_mode(request.mode));
    return client.updateRoleProfile(roleProfileUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a role profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_role_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.deleteRoleProfile(profileId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a user profile by id
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<UserProfileSchema>} user profile schema: id, user_id, name, value, order
 */
export async function read_user_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.readUserProfile(profileId, metadata(server))
        .then(response => get_user_profile_schema(response.toObject().result));
}

/**
 * Read user profiles by user id
 * @param {ServerConfig} server server configuration: address, token
 * @param {UserId} request user id: id
 * @returns {Promise<UserProfileSchema[]>} user profile schema: id, user_id, name, value, order
 */
export async function list_user_profile_by_user(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const userId = new pb_profile.UserId();
    userId.setId(uuid_hex_to_base64(request.id));
    return client.listUserProfile(userId, metadata(server))
        .then(response => get_user_profile_schema_vec(response.toObject().resultsList));
}

/**
 * Create a user profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {UserProfileSchema} request user profile schema: user_id, name, value, order
 * @returns {Promise<ProfileId>} profile id: id
 */
export async function create_user_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const userProfileSchema = new pb_profile.UserProfileSchema();
    userProfileSchema.setUserId(uuid_hex_to_base64(request.user_id));
    userProfileSchema.setName(request.name);
    const value = set_data_value(request.value);
    userProfileSchema.setValueBytes(value.bytes);
    userProfileSchema.setValueType(value.type);
    userProfileSchema.setOrder(request.order);
    return client.createUserProfile(userProfileSchema, metadata(server))
        .then(response => get_profile_id(response.toObject()));
}

/**
 * Update a user profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {UserProfileUpdate} request user update: id, name, value
 * @returns {Promise<{}>} update response
 */
export async function update_user_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const userProfileUpdate = new pb_profile.UserProfileUpdate();
    userProfileUpdate.setId(request.id);
    userProfileUpdate.setName(request.name);
    const value = set_data_value(request.value);
    userProfileUpdate.setValueBytes(value.bytes);
    userProfileUpdate.setValueType(value.type);
    return client.updateUserProfile(userProfileUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a user profile
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_user_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.deleteUserProfile(profileId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Swap a user profile order
 * @param {ServerConfig} server server configuration: address, token
 * @param {UserProfileSwap} request user profile swap: user_id, name, order_1, order_2
 * @returns {Promise<{}>} change response
 */
export async function swap_user_profile(server, request) {
    const client = new pb_profile.ProfileServicePromiseClient(server.address, null, null);
    const userProfileSwap = new pb_profile.UserProfileSwap();
    userProfileSwap.setUserId(uuid_hex_to_base64(request.user_id));
    userProfileSwap.setName(request.name);
    userProfileSwap.setOrder1(request.order_1);
    userProfileSwap.setOrder2(request.order_2);
    return client.swapUserProfile(userProfileSwap, metadata(server))
        .then(response => response.toObject());
}
