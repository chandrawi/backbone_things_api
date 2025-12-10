import pb_api from "../proto/auth/api_grpc_web_pb.js";
import pb_auth from "../proto/auth/auth_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64
} from "../common/utility.js";
import { importKey, encryptMessage } from "./auth.js"


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * @typedef {Object} ServerConfig
 * @property {string} address
 * @property {?string} token
 */

/**
 * @typedef {Object} ApiId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} ApiIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {ApiId}
 */
function getApiId(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} ApiName
 * @property {string} name
 */

/**
 * @typedef {Object} ApiCategory
 * @property {string} category
 */

/**
 * @typedef {Object} ApiOption
 * @property {?string} name
 * @property {?string} category
 */

/**
 * @typedef {Object} ApiSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} address
 * @property {string} category
 * @property {string} description
 * @property {string} password
 * @property {string} access_key
 * @property {ProcedureSchema[]} procedures
 */

/**
 * @param {*} r 
 * @returns {ApiSchema}
 */
function get_api_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        address: r.address,
        category: r.category,
        description: r.description,
        password: r.password,
        access_key: r.accessKey,
        procedures: get_procedure_schema_vec(r.proceduresList)
    };
}

/**
 * @param {*} r 
 * @returns {ApiSchema[]}
 */
function get_api_schema_vec(r) {
    return r.map((v) => {return get_api_schema(v)});
}

/**
 * @typedef {Object} ApiUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} address
 * @property {?string} category
 * @property {?string} description
 * @property {?string} password
 * @property {?string} access_key
 */

/**
 * @typedef {Object} ProcedureId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} ProcedureIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {ProcedureId}
 */
function get_procedure_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} ProcedureName
 * @property {Uuid} api_id
 * @property {string} name
 */

/**
 * @typedef {Object} ProcedureOption
 * @property {?Uuid} api_id
 * @property {?string} name
 */

/**
 * @typedef {Object} ProcedureSchema
 * @property {Uuid} id
 * @property {string} api_id
 * @property {string} name
 * @property {string} description
 * @property {string[]} roles
 */

/**
 * @param {*} r 
 * @returns {ProcedureSchema}
 */
function get_procedure_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        api_id: base64_to_uuid_hex(r.apiId),
        name: r.name,
        description: r.description,
        roles: r.rolesList
    };
}

/**
 * @param {*} r 
 * @returns {ProcedureSchema[]}
 */
function get_procedure_schema_vec(r) {
    return r.map((v) => {return get_procedure_schema(v)});
}

/**
 * @typedef {Object} ProcedureUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} description
 */


/**
 * Read an api by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiId} request api uuid: id
 * @returns {Promise<ApiSchema>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function read_api(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiId = new pb_api.ApiId();
    apiId.setId(uuid_hex_to_base64(request.id));
    return client.readApi(apiId, metadata(server))
        .then(response => response.toObject().result);
}

/**
 * Read an api by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiName} request api name: name
 * @returns {Promise<ApiSchema>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function read_api_by_name(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiName = new pb_api.ApiName();
    apiName.setName(request.name);
    return client.readApiByName(apiName, metadata(server))
        .then(response => get_api_schema(response.toObject().result));
}

/**
 * Read apis by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiIds} request api uuid list: ids
 * @returns {Promise<ApiSchema[]>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function list_api_by_ids(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiIds = new pb_api.ApiIds();
    apiIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listApiByIds(apiIds, metadata(server))
        .then(response => get_api_schema_vec(response.toObject().resultsList));
}

/**
 * Read apis by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiName} request api name: name
 * @returns {Promise<ApiSchema[]>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function list_api_by_name(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiName = new pb_api.ApiName();
    apiName.setName(request.name);
    return client.listApiByName(apiName, metadata(server))
        .then(response => get_api_schema_vec(response.toObject().resultsList));
}

/**
 * Read apis by category
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiCategory} request api category: category
 * @returns {Promise<ApiSchema[]>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function list_api_by_category(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiCategory = new pb_api.ApiCategory();
    apiCategory.setCategory(request.category);
    return client.listApiByCategory(apiCategory, metadata(server))
        .then(response => get_api_schema_vec(response.toObject().resultsList));
}

/**
 * Read apis with options
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiOption} request api option: name, category
 * @returns {Promise<ApiSchema[]>} api schema: id, name, address, category, description, password, access_key, procedures
 */
export async function list_api_option(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiOption = new pb_api.ApiOption();
    apiOption.setName(request.name);
    apiOption.setCategory(request.category);
    return client.listApiOption(apiOption, metadata(server))
        .then(response => get_api_schema_vec(response.toObject().resultsList));
}

/**
 * Create an api
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiSchema} request api schema: id, name, address, category, description, password, access_key
 * @returns {Promise<ApiId>} api id: id
 */
export async function create_api(server, request) {
    const client_auth = new pb_auth.AuthServicePromiseClient(server.address, null, null);
    const apiKeyRequest = new pb_auth.ApiKeyRequest();
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiSchema = new pb_api.ApiSchema();
    apiSchema.setId(uuid_hex_to_base64(request.id));
    apiSchema.setName(request.name);
    apiSchema.setAddress(request.address);
    apiSchema.setCategory(request.category);
    apiSchema.setDescription(request.description);
    const key = await client_auth.apiPasswordKey(apiKeyRequest, metadata(server))
        .then(response => response.toObject().publicKey);
    const pubkey = await importKey(key);
    const ciphertext = await encryptMessage(request.password, pubkey);
    apiSchema.setPassword(ciphertext);
    apiSchema.setAccessKey(request.access_key);
    return client.createApi(apiSchema, metadata(server))
        .then(response => getApiId(response.toObject()));
}

/**
 * Update an api
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiUpdate} request api update: id, name, address, category, description, password, access_key
 * @returns {Promise<{}>} update response
 */
export async function update_api(server, request) {
    const client_auth = new pb_auth.AuthServicePromiseClient(server.address, null, null);
    const apiKeyRequest = new pb_auth.ApiKeyRequest();
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiUpdate = new pb_api.ApiUpdate();
    apiUpdate.setId(uuid_hex_to_base64(request.id));
    apiUpdate.setName(request.name);
    apiUpdate.setAddress(request.address);
    apiUpdate.setCategory(request.category);
    apiUpdate.setDescription(request.description);
    if (request.password) {
        const key = await client_auth.apiPasswordKey(apiKeyRequest, metadata(server))
            .then(response => response.toObject().publicKey);
        const pubkey = await importKey(key);
        const ciphertext = await encryptMessage(request.password, pubkey);
        apiUpdate.setPassword(ciphertext);
    }
    apiUpdate.setAccessKey(request.access_key);
    return client.updateApi(apiUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete an api
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiId} request api uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_api(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiId = new pb_api.ApiId();
    apiId.setId(uuid_hex_to_base64(request.id));
    return client.deleteApi(apiId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read an procedure by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureId} request procedure uuid: id
 * @returns {Promise<ProcedureSchema>} procedure schema: id, api_id, name, description, roles
 */
export async function read_procedure(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureId = new pb_api.ProcedureId();
    procedureId.setId(uuid_hex_to_base64(request.id));
    return client.readProcedure(procedureId, metadata(server))
        .then(response => response.toObject().result);
}

/**
 * Read an procedure by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureName} request procedure name: api_id, name
 * @returns {Promise<ProcedureSchema>} procedure schema: id, api_id, name, description, roles
 */
export async function read_procedure_by_name(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureName = new pb_api.ProcedureName();
    procedureName.setApiId(uuid_hex_to_base64(request.api_id));
    procedureName.setName(request.name);
    return client.readProcedureByName(procedureName, metadata(server))
        .then(response => get_procedure_schema(response.toObject().result));
}

/**
 * Read procedures by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureIds} request procedure uuid list: ids
 * @returns {Promise<ProcedureSchema[]>} procedure schema: id, api_id, name, description, roles
 */
export async function list_procedure_by_ids(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureIds = new pb_api.ProcedureIds();
    procedureIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listProcedureByIds(procedureIds, metadata(server))
        .then(response => get_procedure_schema_vec(response.toObject().resultsList));
}

/**
 * Read procedures by api uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ApiId} request api uuid: id
 * @returns {Promise<ProcedureSchema[]>} procedure schema: id, api_id, name, description, roles
 */
export async function list_procedure_by_api(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const apiId = new pb_api.ApiId();
    apiId.setId(uuid_hex_to_base64(request.id));
    return client.listProcedureByApi(apiId, metadata(server))
        .then(response => get_procedure_schema_vec(response.toObject().resultsList));
}

/**
 * Read procedures by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureName} request procedure name: name
 * @returns {Promise<ProcedureSchema[]>} procedure schema: id, api_id, name, description, roles
 */
export async function list_procedure_by_name(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureName = new pb_api.ProcedureName();
    procedureName.setName(request.name);
    return client.listProcedureByName(procedureName, metadata(server))
        .then(response => get_procedure_schema_vec(response.toObject().resultsList));
}

/**
 * Read procedures with options
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureOption} request procedure option: api_id, name
 * @returns {Promise<ProcedureSchema[]>} procedure schema: id, api_id, name, description, roles
 */
export async function list_procedure_option(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureOption = new pb_api.ProcedureOption();
    if (request.api_id) {
        procedureOption.setApiId(uuid_hex_to_base64(request.api_id))
    }
    procedureOption.setName(request.name);
    return client.listProcedureOption(procedureOption, metadata(server))
        .then(response => get_procedure_schema_vec(response.toObject().resultsList));
}

/**
 * Create a procedure
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureSchema} request procedure schema: id, api_id, name, description
 * @returns {Promise<ProcedureId>} procedure id: id
 */
export async function create_procedure(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureSchema = new pb_api.ProcedureSchema();
    procedureSchema.setId(uuid_hex_to_base64(request.id));
    procedureSchema.setApiId(uuid_hex_to_base64(request.api_id));
    procedureSchema.setName(request.name);
    procedureSchema.setDescription(request.description);
    return client.createProcedure(procedureSchema, metadata(server))
        .then(response => get_procedure_id(response.toObject()));
}

/**
 * Update a procedure
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureUpdate} request procedure update: id, name, description
 * @returns {Promise<{}>} update response
 */
export async function update_procedure(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureUpdate = new pb_api.ProcedureUpdate();
    procedureUpdate.setId(uuid_hex_to_base64(request.id));
    procedureUpdate.setName(request.name);
    procedureUpdate.setDescription(request.description);
    return client.updateProcedure(procedureUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a procedure
 * @param {ServerConfig} server server configuration: address, token
 * @param {ProcedureId} request procedure uuid: id
 * @returns {Promise<{}>} update response
 */
export async function delete_procedure(server, request) {
    const client = new pb_api.ApiServicePromiseClient(server.address, null, null);
    const procedureId = new pb_api.ProcedureId();
    procedureId.setId(uuid_hex_to_base64(request.id));
    return client.deleteProcedure(procedureId, metadata(server))
        .then(response => response.toObject());
}
