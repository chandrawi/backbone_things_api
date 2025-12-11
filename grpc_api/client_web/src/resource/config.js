import pb_config from "../proto/resource/config_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex
} from "../common/utility.js";


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * @typedef {Object} ServerConfig
 * @property {string} address
 * @property {?string} access_token
 */

/**
 * @typedef {Object} ApiId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} ProcedureAcces
 * @property {string} procedure
 * @property {string[]} roles
 */

/**
 * @param {*} r 
 * @returns {ProcedureAcces}
 */
function get_procedure_acces(r) {
    return {
        procedure: r.procedure,
        roles: r.rolesList
    };
}

/**
 * @param {*} r 
 * @returns {ProcedureAcces[]}
 */
function get_procedure_acces_vec(r) {
    return r.map((v) => {return get_procedure_acces(v)});
}

/**
 * @typedef {Object} RoleAcces
 * @property {string} role
 * @property {string[]} procedures
 */

/**
 * @param {*} r 
 * @returns {RoleAcces}
 */
function get_role_acces(r) {
    return {
        role: r.role,
        procedures: r.proceduresList
    };
}

/**
 * @param {*} r 
 * @returns {RoleAcces[]}
 */
function get_role_acces_vec(r) {
    return r.map((v) => {return get_role_acces(v)});
}


/**
 * Get a resource server api id
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {{}} request
 * @returns {Promise<Uuid>} resource api id
 */
export async function api_id(config, request) {
    const client = new pb_config.ConfigServicePromiseClient(config.address, null, null);
    const apiIdRequest = new pb_config.ApiIdRequest();
    return client.ApiId(apiIdRequest, metadata(config.access_token))
        .then(response => base64_to_uuid_hex(response.toObject().api_id));
}

/**
 * Get a resource server procedure access map
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {{}} request
 * @returns {Promise<ProcedureAcces[]>} procedure access: procedure, roles
 */
export async function procedure_access(config, request) {
    const client = new pb_config.ConfigServicePromiseClient(config.address, null, null);
    const accessRequest = new pb_config.AccessRequest();
    return client.ProcedureAcces(accessRequest, metadata(config.access_token))
        .then(response => get_procedure_acces_vec(response.toObject().access));
}

/**
 * Get a resource server role access map
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {{}} request
 * @returns {Promise<RoleAcces[]>} role access: role, procedures
 */
export async function role_access(config, request) {
    const client = new pb_config.ConfigServicePromiseClient(config.address, null, null);
    const accessRequest = new pb_config.AccessRequest();
    return client.RoleAcces(accessRequest, metadata(config.access_token))
        .then(response => get_role_acces_vec(response.toObject().access));
}
