import pb_user from "../proto/auth/user_grpc_web_pb.js";
import pb_auth from "../proto/auth/auth_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64,
    importKey,
    encryptMessage
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
 * @typedef {Object} ApiId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} UserId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} UserIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {UserId}
 */
function get_user_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} UserName
 * @property {string} name
 */

/**
 * @typedef {Object} RoleId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} UserOption
 * @property {?Uuid} api_id
 * @property {?Uuid} role_id
 * @property {?name} name
 */

/**
 * @typedef {Object} UserRoleSchema
 * @property {Uuid} api_id
 * @property {string} role
 * @property {boolean} multi
 * @property {boolean} ip_lock
 * @property {number} access_duration
 * @property {number} refresh_duration
 * @property {string} access_key
 */

/**
 * @typedef {Object} UserSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} email
 * @property {string} phone
 * @property {string} password
 * @property {UserRoleSchema[]} roles
 */

/**
 * @param {*} r 
 * @returns {UserRoleSchema}
 */
function get_user_role_schema(r) {
    return {
        api_id: base64_to_uuid_hex(r.apiId),
        role: r.role,
        multi: r.multi,
        ip_lock: r.ipLock,
        access_duration: r.accessDuration,
        refresh_duration: r.refreshDuration,
        access_key: r.accessKey
    };
}

/**
 * @param {*} r 
 * @returns {UserSchema}
 */
function get_user_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        email: r.email,
        phone: r.phone,
        password: r.password,
        roles: r.rolesList.map((v) => {return get_user_role_schema(v)})
    };
}

/**
 * @param {*} r 
 * @returns {UserSchema[]}
 */
function get_user_schema_vec(r) {
    return r.map((v) => {return get_user_schema(v)});
}

/**
 * @typedef {Object} UserUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} email
 * @property {?string} phone
 * @property {?string} password
 */

/**
 * @typedef {Object} UserRole
 * @property {Uuid} user_id
 * @property {Uuid} role_id
 */


/**
 * Read a user by uuid
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserId} request user uuid: id
 * @returns {Promise<UserSchema>} user schema: id, name, email, phone, password, roles
 */
export async function read_user(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userId = new pb_user.UserId();
    userId.setId(uuid_hex_to_base64(request.id));
    return client.readUser(userId, metadata(config.auth_token))
        .then(response => get_user_schema(response.toObject().result));
}

/**
 * Read a user by name
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserName} request user name: name
 * @returns {Promise<UserSchema>} user schema: id, name, email, phone, password, roles
 */
export async function read_user_by_name(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userName = new pb_user.UserName();
    userName.setName(request.name);
    return client.readUserByName(userName, metadata(config.auth_token))
        .then(response => get_user_schema(response.toObject().result));
}

/**
 * Read users by uuid list
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserIds} request user uuid list: ids
 * @returns {Promise<UserSchema[]>} user schema: id, name, email, phone, password, roles
 */
export async function list_user_by_ids(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userIds = new pb_user.UserIds();
    userIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listUserByIds(userIds, metadata(config.auth_token))
        .then(response => get_user_schema_vec(response.toObject().resultsList));
}

/**
 * Read users by api uuid
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {ApiId} request api uuid: id
 * @returns {Promise<UserSchema[]>} user schema: id, name, email, phone, password, roles
 */
export async function list_user_by_api(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const apiId = new pb_user.ApiId();
    apiId.setId(uuid_hex_to_base64(request.id));
    return client.listUserByApi(apiId, metadata(config.auth_token))
        .then(response => get_user_schema_vec(response.toObject().resultsList));
}

/**
 * Read users by role uuid
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {RoleId} request role uuid: id
 * @returns {Promise<UserSchema[]>} user schema: id, name, email, phone, password, roles
 */
export async function list_user_by_role(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const roleId = new pb_user.RoleId();
    roleId.setId(uuid_hex_to_base64(request.id));
    return client.listUserByRole(roleId, metadata(config.auth_token))
        .then(response => get_user_schema_vec(response.toObject().resultsList));
}

/**
 * Read users by name
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserName} request user name: name
 * @returns {Promise<UserSchema[]>} user schema: id, name, email, phone, password, roles
 */
export async function list_user_by_name(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userName = new pb_user.UserName();
    userName.setName(request.name);
    return client.listUserByName(userName, metadata(config.auth_token))
        .then(response => get_user_schema_vec(response.toObject().resultsList));
}

/**
 * Read users with options
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserOption} request user option: api_id, role_id, name
 * @returns {Promise<UserSchema[]>} user schema: id, name, email, phone, password, roles
 */
export async function list_user_option(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userOption = new pb_user.UserOption();
    if (request.api_id) {
        userOption.setApiId(uuid_hex_to_base64(request.api_id))
    }
    if (request.role_id) {
        userOption.setApiId(uuid_hex_to_base64(request.role_id))
    }
    userOption.setName(request.name);
    return client.listUserOption(userOption, metadata(config.auth_token))
        .then(response => get_user_schema_vec(response.toObject().resultsList));
}

/**
 * Create an user
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserSchema} request user schema: id, name, email, phone, password
 * @returns {Promise<UserId>} user id: id
 */
export async function create_user(config, request) {
    const client_auth = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userKeyRequest = new pb_auth.UserKeyRequest();
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userSchema = new pb_user.UserSchema();
    userSchema.setId(uuid_hex_to_base64(request.id));
    userSchema.setName(request.name);
    userSchema.setEmail(request.email);
    userSchema.setPhone(request.phone);
    const key = await client_auth.userPasswordKey(userKeyRequest, metadata(config.auth_token))
        .then(response => response.toObject().publicKey);
    const pubkey = await importKey(key);
    const ciphertext = await encryptMessage(request.password, pubkey);
    userSchema.setPassword(ciphertext);
    return client.createUser(userSchema, metadata(config.auth_token))
        .then(response => get_user_id(response.toObject()));
}

/**
 * Update an user
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserUpdate} request user update: id, name, email, phone, password
 * @returns {Promise<{}>} update response
 */
export async function update_user(config, request) {
    const client_auth = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userKeyRequest = new pb_auth.UserKeyRequest();
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userUpdate = new pb_user.UserUpdate();
    userUpdate.setId(uuid_hex_to_base64(request.id));
    userUpdate.setName(request.name);
    userUpdate.setEmail(request.email);
    userUpdate.setPhone(request.phone);
    if (request.password) {
        const key = await client_auth.userPasswordKey(userKeyRequest, metadata(config.auth_token))
            .then(response => response.toObject().publicKey);
        const pubkey = await importKey(key);
        const ciphertext = await encryptMessage(request.password, pubkey);
        userUpdate.setPassword(ciphertext);
    }
    return client.updateUser(userUpdate, metadata(config.auth_token))
        .then(response => response.toObject());
}

/**
 * Delete an user
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserId} request user uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_user(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userId = new pb_user.UserId();
    userId.setId(uuid_hex_to_base64(request.id));
    return client.deleteUser(userId, metadata(config.auth_token))
        .then(response => response.toObject());
}

/**
 * Add a role to user
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserRole} request user role: user_id, role_id
 * @returns {Promise<{}>} change response
 */
export async function add_user_role(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userRole = new pb_user.UserRole();
    userRole.setUserId(uuid_hex_to_base64(request.user_id));
    userRole.setRoleId(uuid_hex_to_base64(request.role_id));
    return client.addUserRole(userRole, metadata(config.auth_token))
        .then(response => response.toObject());
}

/**
 * Remove a role from user
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserRole} request user role: user_id, role_id
 * @returns {Promise<{}>} change response
 */
export async function remove_user_role(config, request) {
    const client = new pb_user.UserServicePromiseClient(config.address, null, null);
    const userRole = new pb_user.UserRole();
    userRole.setUserId(uuid_hex_to_base64(request.user_id));
    userRole.setRoleId(uuid_hex_to_base64(request.role_id));
    return client.removeUserRole(userRole, metadata(config.auth_token))
        .then(response => response.toObject());
}
