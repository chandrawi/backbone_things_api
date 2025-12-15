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
 * @property {?string} auth_token
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
 * @typedef {Object} RoleProfileSchema
 * @property {number} id
 * @property {Uuid} role_id
 * @property {string} name
 * @property {number|string} value_type
 * @property {number|bigint|string|Uint8Array|boolean} value_default
 * @property {string} category
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
        value_default: get_data_value(r.valueBytes, r.valueType),
        category: r.category
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
 * @property {string} category
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
        category: r.category
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
 * @property {?number|bigint|string|Uint8Array|boolean} value_default
 * @property {?string} category
 */

/**
 * @typedef {Object} UserProfileUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|bigint|string|Uint8Array|boolean} value
 * @property {?string} category
 */


/**
 * Read a role profile by id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<RoleProfileSchema>} role profile schema: id, role_id, name, value_type, value_default, category
 */
export async function read_role_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.readRoleProfile(profileId, metadata(config.auth_token))
        .then(response => get_role_profile_schema(response.toObject().result));
}

/**
 * Read role profiles by role id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {RoleId} request role id: id
 * @returns {Promise<RoleProfileSchema[]>} role profile schema: id, role_id, name, value_type, value_default, category
 */
export async function list_role_profile_by_role(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const roleId = new pb_profile.RoleId();
    roleId.setId(uuid_hex_to_base64(request.id));
    return client.listRoleProfile(roleId, metadata(config.auth_token))
        .then(response => get_role_profile_schema_vec(response.toObject().resultsList));
}

/**
 * Create a role profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {RoleProfileSchema} request role profile schema: role_id, name, value_type, value_default, category
 * @returns {Promise<number>} profile id
 */
export async function create_role_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const roleProfileSchema = new pb_profile.RoleProfileSchema();
    roleProfileSchema.setRoleId(uuid_hex_to_base64(request.role_id));
    roleProfileSchema.setName(request.name);
    roleProfileSchema.setValueType(set_data_type(request.value_type));
    const value = set_data_value(request.value_default);
    roleProfileSchema.setValueBytes(value.bytes);
    roleProfileSchema.setCategory(request.category);
    return client.createRoleProfile(roleProfileSchema, metadata(config.auth_token))
        .then(response => response.toObject().id);
}

/**
 * Update a role profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {RoleProfileUpdate} request role update: id, name, value_type, value_default, category
 * @returns {Promise<null>} update response
 */
export async function update_role_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const roleProfileUpdate = new pb_profile.RoleProfileUpdate();
    roleProfileUpdate.setId(request.id);
    roleProfileUpdate.setName(request.name);
    roleProfileUpdate.setValueType(set_data_type(request.value_type));
    const value = set_data_value(request.value_default);
    roleProfileUpdate.setValueBytes(value.bytes);
    roleProfileUpdate.setCategory(request.category);
    return client.updateRoleProfile(roleProfileUpdate, metadata(config.auth_token))
        .then(response => null);
}

/**
 * Delete a role profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_role_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.deleteRoleProfile(profileId, metadata(config.auth_token))
        .then(response => null);
}

/**
 * Read a user profile by id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<UserProfileSchema>} user profile schema: id, user_id, name, value, category
 */
export async function read_user_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.readUserProfile(profileId, metadata(config.auth_token))
        .then(response => get_user_profile_schema(response.toObject().result));
}

/**
 * Read user profiles by user id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserId} request user id: id
 * @returns {Promise<UserProfileSchema[]>} user profile schema: id, user_id, name, value, category
 */
export async function list_user_profile_by_user(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const userId = new pb_profile.UserId();
    userId.setId(uuid_hex_to_base64(request.id));
    return client.listUserProfile(userId, metadata(config.auth_token))
        .then(response => get_user_profile_schema_vec(response.toObject().resultsList));
}

/**
 * Create a user profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserProfileSchema} request user profile schema: user_id, name, value, category
 * @returns {Promise<number>} profile id
 */
export async function create_user_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const userProfileSchema = new pb_profile.UserProfileSchema();
    userProfileSchema.setUserId(uuid_hex_to_base64(request.user_id));
    userProfileSchema.setName(request.name);
    const value = set_data_value(request.value);
    userProfileSchema.setValueBytes(value.bytes);
    userProfileSchema.setValueType(value.type);
    userProfileSchema.setCategory(request.category);
    return client.createUserProfile(userProfileSchema, metadata(config.auth_token))
        .then(response => response.toObject().id);
}

/**
 * Update a user profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserProfileUpdate} request user update: id, name, value, category
 * @returns {Promise<null>} update response
 */
export async function update_user_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const userProfileUpdate = new pb_profile.UserProfileUpdate();
    userProfileUpdate.setId(request.id);
    userProfileUpdate.setName(request.name);
    const value = set_data_value(request.value);
    userProfileUpdate.setValueBytes(value.bytes);
    userProfileUpdate.setValueType(value.type);
    userProfileUpdate.setCategory(request.category);
    return client.updateUserProfile(userProfileUpdate, metadata(config.auth_token))
        .then(response => null);
}

/**
 * Delete a user profile
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {ProfileId} request profile id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_user_profile(config, request) {
    const client = new pb_profile.ProfileServicePromiseClient(config.address, null, null);
    const profileId = new pb_profile.ProfileId();
    profileId.setId(request.id);
    return client.deleteUserProfile(profileId, metadata(config.auth_token))
        .then(response => null);
}
