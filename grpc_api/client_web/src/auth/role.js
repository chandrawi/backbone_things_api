import pb_role from "../proto/auth/role_grpc_web_pb.js";
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
 * @typedef {Object} RoleId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} RoleIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {RoleId}
 */
function get_role_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} RoleName
 * @property {Uuid} api_id
 * @property {string} name
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
 * @typedef {Object} RoleOption
 * @property {?Uuid} api_id
 * @property {?Uuid} user_id
 * @property {?string} name
 */

/**
 * @typedef {Object} RoleSchema
 * @property {Uuid} id
 * @property {Uuid} api_id
 * @property {string} name
 * @property {boolean} multi
 * @property {boolean} ip_lock
 * @property {number} access_duration
 * @property {number} refresh_duration
 * @property {string} access_key
 * @property {string[]} procedures
 */

/**
 * @param {*} r 
 * @returns {RoleSchema}
 */
function get_role_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        api_id: base64_to_uuid_hex(r.apiId),
        name: r.name,
        multi: r.multi,
        ip_lock: r.ipLock,
        access_duration: r.accessDuration,
        refresh_duration: r.refreshDuration,
        access_key: r.accessKey,
        procedures: r.proceduresList.map((v) => {return base64_to_uuid_hex(v)})
    };
}

/**
 * @param {*} r 
 * @returns {RoleSchema[]}
 */
function get_role_schema_vec(r) {
    return r.map((v) => {return get_role_schema(v)});
}

/**
 * @typedef {Object} RoleUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?boolean} multi
 * @property {?boolean} ip_lock
 * @property {?number} access_duration
 * @property {?number} refresh_duration
 */

/**
 * @typedef {Object} RoleAccess
 * @property {Uuid} id
 * @property {Uuid} procedure_id
 */


/**
 * Read a role by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleId} request role uuid: id
 * @returns {Promise<RoleSchema>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function read_role(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleId = new pb_role.RoleId();
    roleId.setId(uuid_hex_to_base64(request.id));
    return client.readRole(roleId, metadata(server))
        .then(response => get_role_schema(response.toObject().result));
}

/**
 * Read a role by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleName} request role name: api_id, name
 * @returns {Promise<RoleSchema>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function read_role_by_name(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleName = new pb_role.RoleName();
    roleName.setApiId(uuid_hex_to_base64(request.api_id));
    roleName.setName(request.name);
    return client.readRoleByName(roleName, metadata(server))
        .then(response => get_role_schema(response.toObject().result));
}

/**
 * Read roles by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleIds} request role uuid list: ids
 * @returns {Promise<RoleSchema[]>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function list_role_by_ids(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleIds = new pb_role.RoleIds();
    roleIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listRoleByIds(roleIds, metadata(server))
        .then(response => get_role_schema_vec(response.toObject().resultsList));
}

/**
 * Read roles by api uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiId} request api uuid: id
 * @returns {Promise<RoleSchema[]>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function list_role_by_api(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const apiId = new pb_role.ApiId();
    apiId.setApiId(uuid_hex_to_base64(request.id));
    return client.listRoleByApi(apiId, metadata(server))
        .then(response => get_role_schema_vec(response.toObject().resultsList));
}

/**
 * Read roles by user uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {UserId} request user uuid: id
 * @returns {Promise<RoleSchema[]>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function list_role_by_user(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const userId = new pb_role.UserId();
    userId.setUserId(uuid_hex_to_base64(request.id));
    return client.listRoleByUser(userId, metadata(server))
        .then(response => get_role_schema_vec(response.toObject().resultsList));
}

/**
 * Read roles by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleName} request role name: name
 * @returns {Promise<RoleSchema[]>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function list_role_by_name(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleName = new pb_role.RoleName();
    roleName.setName(request.name);
    return client.listRoleByName(roleName, metadata(server))
        .then(response => get_role_schema_vec(response.toObject().resultsList));
}

/**
 * Read roles with options
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleOption} request role option: api_id, user_id, name
 * @returns {Promise<RoleSchema[]>} role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key, procedures
 */
export async function list_role_option(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleOption = new pb_role.RoleOption();
    if (request.api_id) {
        roleOption.setApiId(uuid_hex_to_base64(request.api_id))
    }
    if (request.user_id) {
        roleOption.setUserId(uuid_hex_to_base64(request.user_id))
    }
    roleOption.setName(request.name);
    return client.listRoleOption(roleOption, metadata(server))
        .then(response => get_role_schema_vec(response.toObject().resultsList));
}

/**
 * Create a role
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleSchema} request role schema: id, api_id, name, multi, ip_lock, access_duration, refresh_duration, access_key
 * @returns {Promise<RoleId>} role id: id
 */
export async function create_role(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleSchema = new pb_role.RoleSchema();
    roleSchema.setId(uuid_hex_to_base64(request.id));
    roleSchema.setApiId(uuid_hex_to_base64(request.api_id));
    roleSchema.setName(request.name);
    roleSchema.setMulti(request.multi);
    roleSchema.setIpLock(request.ip_lock);
    roleSchema.setAccessDuration(request.access_duration);
    roleSchema.setRefreshDuration(request.refresh_duration);
    roleSchema.setAccessKey(request.access_key);
    return client.createRole(roleSchema, metadata(server))
        .then(response => get_role_id(response.toObject()));
}

/**
 * Update a role
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleUpdate} request role update: id, name, multi, ip_lock, access_duration, refresh_duration
 * @returns {Promise<{}>} update response
 */
export async function update_role(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleUpdate = new pb_role.RoleUpdate();
    roleUpdate.setId(uuid_hex_to_base64(request.id));
    roleUpdate.setName(request.name);
    roleUpdate.setMulti(request.multi);
    roleUpdate.setIpLock(request.ip_lock);
    roleUpdate.setAccessDuration(request.access_duration);
    roleUpdate.setRefreshDuration(request.refresh_duration);
    return client.updateRole(roleUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a role
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleId} request role uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_role(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleId = new pb_role.RoleId();
    roleId.setId(uuid_hex_to_base64(request.id));
    return client.deleteRole(roleId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Add a role access
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleAccess} request role access: id, procedure_id
 * @returns {Promise<{}>} change response
 */
export async function add_role_access(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleAccess = new pb_role.RoleAccess();
    roleAccess.setId(uuid_hex_to_base64(request.id));
    roleAccess.setProcedureId(uuid_hex_to_base64(request.procedure_id));
    return client.addRoleAccess(roleAccess, metadata(server))
        .then(response => response.toObject());
}

/**
 * Remove a role access
 * @param {ServerConfig} server server configuration: address, token
 * @param {RoleAccess} request role access: id, procedure_id
 * @returns {Promise<{}>} change response
 */
export async function remove_role_access(server, request) {
    const client = new pb_role.RoleServicePromiseClient(server.address, null, null);
    const roleAccess = new pb_role.RoleAccess();
    roleAccess.setId(uuid_hex_to_base64(request.id));
    roleAccess.setProcedureId(uuid_hex_to_base64(request.procedure_id));
    return client.removeRoleAccess(roleAccess, metadata(server))
        .then(response => response.toObject());
}
