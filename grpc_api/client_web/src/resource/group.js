import pb_group from '../proto/resource/group_grpc_web_pb.js';
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
 * @typedef {Object} GroupId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} GroupIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {GroupId}
 */
function get_group_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} GroupName
 * @property {string} name
 */

/**
 * @typedef {Object} GroupCategory
 * @property {string} category
 */

/**
 * @typedef {Object} GroupOption
 * @property {?string} name
 * @property {?string} category
 */

/**
 * @typedef {Object} GroupModelSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} category
 * @property {string} description
 * @property {Uuid[]} model_ids
 */

/**
 * @typedef {Object} GroupDeviceSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} category
 * @property {string} description
 * @property {Uuid[]} device_ids
 */

/**
 * @typedef {Object} GroupGatewaySchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} category
 * @property {string} description
 * @property {Uuid[]} gateway_ids
 */

/**
 * @param {*} r 
 * @returns {GroupModelSchema}
 */
export function get_group_model_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        category: r.category,
        description: r.description,
        model_ids: r.modelIdsList.map((v) => {return base64_to_uuid_hex(v)})
    };
}

/**
 * @param {*} r 
 * @returns {GroupModelSchema[]}
 */
function get_group_model_schema_vec(r) {
    return r.map((v) => {return get_group_model_schema(v)});
}

/**
 * @param {*} r 
 * @returns {GroupDeviceSchema}
 */
export function get_group_device_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        category: r.category,
        description: r.description,
        device_ids: r.deviceIdsList.map((v) => {return base64_to_uuid_hex(v)})
    };
}

/**
 * @param {*} r 
 * @returns {GroupDeviceSchema[]}
 */
function get_group_device_schema_vec(r) {
    return r.map((v) => {return get_group_device_schema(v)});
}

/**
 * @param {*} r 
 * @returns {GroupGatewaySchema}
 */
export function get_group_gateway_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        category: r.category,
        description: r.description,
        gateway_ids: r.deviceIdsList.map((v) => {return base64_to_uuid_hex(v)})
    };
}

/**
 * @param {*} r 
 * @returns {GroupGatewaySchema[]}
 */
function get_group_gateway_schema_vec(r) {
    return r.map((v) => {return get_group_gateway_schema(v)});
}

/**
 * @typedef {Object} GroupUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} category
 * @property {?string} description
 */

/**
 * @typedef {Object} GroupModel
 * @property {Uuid} id
 * @property {Uuid} model_id
 */

/**
 * @typedef {Object} GroupDevice
 * @property {Uuid} id
 * @property {Uuid} device_id
 */

/**
 * @typedef {Object} GroupGateway
 * @property {Uuid} id
 * @property {Uuid} gateway_id
 */


/**
 * Read a group model by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group model uuid: id
 * @returns {Promise<GroupModelSchema>} group model schema: id, name, category, description, model_ids
 */
export async function read_group_model(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.readGroupModel(groupId, metadata(server))
        .then(response => get_group_model_schema(response.toObject().result));
}

/**
 * Read groups of model by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupIds} request group uuid list: ids
 * @returns {Promise<GroupModelSchema[]>} group model schema: id, name, category, description, model_ids
 */
export async function list_group_model_by_ids(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupIds = new pb_group.GroupIds();
    groupIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listGroupModelByIds(groupIds, metadata(server))
        .then(response => get_group_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of model by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupName} request group model name: name
 * @returns {Promise<GroupModelSchema[]>} group model schema: id, name, category, description, model_ids
 */
export async function list_group_model_by_name(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupName = new pb_group.GroupName();
    groupName.setName(request.name);
    return client.listGroupModelByName(groupName, metadata(server))
        .then(response => get_group_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of model by category
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupCategory} request group model category: category
 * @returns {Promise<GroupModelSchema[]>} group model schema: id, name, category, description, model_ids
 */
export async function list_group_model_by_category(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupCategory = new pb_group.GroupCategory();
    groupCategory.setCategory(request.category);
    return client.listGroupModelByCategory(groupCategory, metadata(server))
        .then(response => get_group_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of model with select option
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupOption} request group model option: name, category
 * @returns {Promise<GroupModelSchema[]>} group model schema: id, name, category, description, model_ids
 */
export async function list_group_model_option(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupOption = new pb_group.GroupOption();
    groupOption.setName(request.name);
    groupOption.setCategory(request.category);
    return client.listGroupModelOption(groupOption, metadata(server))
        .then(response => get_group_model_schema_vec(response.toObject().resultsList));
}

/**
 * Create a group model
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupModelSchema} request group model schema: id, name, category, description
 * @returns {Promise<GroupId>} group model uuid: id
 */
export async function create_group_model(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupSchema = new pb_group.GroupModelSchema();
    groupSchema.setId(uuid_hex_to_base64(request.id));
    groupSchema.setName(request.name);
    groupSchema.setCategory(request.category);
    groupSchema.setDescription(request.description);
    return client.createGroupModel(groupSchema, metadata(server))
        .then(response => get_group_id(response.toObject()));
}

/**
 * Update a group model
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupUpdate} request group model update: id, name, category, description
 * @returns {Promise<{}>} update response
 */
export async function update_group_model(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupUpdate = new pb_group.GroupUpdate();
    groupUpdate.setId(uuid_hex_to_base64(request.id));
    groupUpdate.setName(request.name);
    groupUpdate.setCategory(request.category);
    groupUpdate.setDescription(request.description);
    return client.updateGroupModel(groupUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a group model
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group model uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_group_model(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.deleteGroupModel(groupId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Add a member to a group model
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupModel} request group model member: id, model_id
 * @returns {Promise<{}>} change response
 */
export async function add_group_model_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupModel = new pb_group.GroupModel();
    groupModel.setId(uuid_hex_to_base64(request.id));
    groupModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.addGroupModelMember(groupModel, metadata(server))
        .then(response => response.toObject());
}

/**
 * Remove a member to a group model
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupModel} request group model member: id, model_id
 * @returns {Promise<{}>} change response
 */
export async function remove_group_model_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupModel = new pb_group.GroupModel();
    groupModel.setId(uuid_hex_to_base64(request.id));
    groupModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.removeGroupModelMember(groupModel, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a group device by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group device uuid: id
 * @returns {Promise<GroupDeviceSchema>} group device schema: id, name, category, description, device_ids
 */
export async function read_group_device(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.readGroupDevice(groupId, metadata(server))
        .then(response => get_group_device_schema(response.toObject().result));
}

/**
 * Read groups of device by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupIds} request group uuid list: ids
 * @returns {Promise<GroupDeviceSchema[]>} group device schema: id, name, category, description, device_ids
 */
export async function list_group_device_by_ids(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupIds = new pb_group.GroupIds();
    groupIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listGroupDeviceByIds(groupIds, metadata(server))
        .then(response => get_group_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of device by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupName} request group device name: name
 * @returns {Promise<GroupDeviceSchema[]>} group device schema: id, name, category, description, device_ids
 */
export async function list_group_device_by_name(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupName = new pb_group.GroupName();
    groupName.setName(request.name);
    return client.listGroupDeviceByName(groupName, metadata(server))
        .then(response => get_group_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of device by category
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupCategory} request group device category: category
 * @returns {Promise<GroupDeviceSchema[]>} group device schema: id, name, category, description, device_ids
 */
export async function list_group_device_by_category(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupCategory = new pb_group.GroupCategory();
    groupCategory.setCategory(request.category);
    return client.listGroupDeviceByCategory(groupCategory, metadata(server))
        .then(response => get_group_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of device with select options
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupOption} request group device option: name, category
 * @returns {Promise<GroupDeviceSchema[]>} group device schema: id, name, category, description, device_ids
 */
export async function list_group_device_option(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupOption = new pb_group.GroupOption();
    groupOption.setName(request.name);
    groupOption.setCategory(request.category);
    return client.listGroupDeviceOption(groupOption, metadata(server))
        .then(response => get_group_device_schema_vec(response.toObject().resultsList));
}

/**
 * Create a group device
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupDeviceSchema} request group device schema: id, name, category, description
 * @returns {Promise<GroupId>} group device uuid: id
 */
export async function create_group_device(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupSchema = new pb_group.GroupDeviceSchema();
    groupSchema.setId(uuid_hex_to_base64(request.id));
    groupSchema.setName(request.name);
    groupSchema.setCategory(request.category);
    groupSchema.setDescription(request.description);
    return client.createGroupDevice(groupSchema, metadata(server))
        .then(response => get_group_id(response.toObject()));
}

/**
 * Update a group device
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupUpdate} request group device update: id, name, category, description
 * @returns {Promise<{}>} update response
 */
export async function update_group_device(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupUpdate = new pb_group.GroupUpdate();
    groupUpdate.setId(uuid_hex_to_base64(request.id));
    groupUpdate.setName(request.name);
    groupUpdate.setCategory(request.category);
    groupUpdate.setDescription(request.description);
    return client.updateGroupDevice(groupUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a group device
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group device uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_group_device(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.deleteGroupDevice(groupId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Add a member to a group device
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupDevice} request group device member: id, device_id
 * @returns {Promise<{}>} change response
 */
export async function add_group_device_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupDevice = new pb_group.GroupDevice();
    groupDevice.setId(uuid_hex_to_base64(request.id));
    groupDevice.setDeviceId(uuid_hex_to_base64(request.device_id));
    return client.addGroupDeviceMember(groupDevice, metadata(server))
        .then(response => response.toObject());
}

/**
 * Remove a member to a group device
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupDevice} request group device member: id, device_id
 * @returns {Promise<{}>} change response
 */
export async function remove_group_device_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupDevice = new pb_group.GroupDevice();
    groupDevice.setId(uuid_hex_to_base64(request.id));
    groupDevice.setDeviceId(uuid_hex_to_base64(request.device_id));
    return client.removeGroupDeviceMember(groupDevice, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a group gateway by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group gateway uuid: id
 * @returns {Promise<GroupGatewaySchema>} group gateway schema: id, name, category, description, gateway_ids
 */
export async function read_group_gateway(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.readGroupGateway(groupId, metadata(server))
        .then(response => get_group_gateway_schema(response.toObject().result));
}

/**
 * Read groups of gateway by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupIds} request group uuid list: ids
 * @returns {Promise<GroupGatewaySchema[]>} group gateway schema: id, name, category, description, gateway_ids
 */
export async function list_group_gateway_by_ids(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupIds = new pb_group.GroupIds();
    groupIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listGroupGatewayByIds(groupIds, metadata(server))
        .then(response => get_group_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of gateway by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupName} request group gateway name: name
 * @returns {Promise<GroupGatewaySchema[]>} group gateway schema: id, name, category, description, gateway_ids
 */
export async function list_group_gateway_by_name(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupName = new pb_group.GroupName();
    groupName.setName(request.name);
    return client.listGroupGatewayByName(groupName, metadata(server))
        .then(response => get_group_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of gateway by category
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupCategory} request group gateway category: category
 * @returns {Promise<GroupGatewaySchema[]>} group gateway schema: id, name, category, description, gateway_ids
 */
export async function list_group_gateway_by_category(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupCategory = new pb_group.GroupCategory();
    groupCategory.setCategory(request.category);
    return client.listGroupGatewayByCategory(groupCategory, metadata(server))
        .then(response => get_group_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read groups of gateway with select options
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupOption} request group gateway option: name, category
 * @returns {Promise<GroupGatewaySchema[]>} group gateway schema: id, name, category, description, gateway_ids
 */
export async function list_group_gateway_option(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupOption = new pb_group.GroupOption();
    groupOption.setName(request.name);
    groupOption.setCategory(request.category);
    return client.listGroupGatewayOption(groupOption, metadata(server))
        .then(response => get_group_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Create a group gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupGatewaySchema} request group gateway schema: id, name, category, description
 * @returns {Promise<GroupId>} group gateway uuid: id
 */
export async function create_group_gateway(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupSchema = new pb_group.GroupDeviceSchema();
    groupSchema.setId(uuid_hex_to_base64(request.id));
    groupSchema.setName(request.name);
    groupSchema.setCategory(request.category);
    groupSchema.setDescription(request.description);
    return client.createGroupGateway(groupSchema, metadata(server))
        .then(response => get_group_id(response.toObject()));
}

/**
 * Update a group gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupUpdate} request group gateway update: id, name, category, description
 * @returns {Promise<{}>} update response
 */
export async function update_group_gateway(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupUpdate = new pb_group.GroupUpdate();
    groupUpdate.setId(uuid_hex_to_base64(request.id));
    groupUpdate.setName(request.name);
    groupUpdate.setCategory(request.category);
    groupUpdate.setDescription(request.description);
    return client.updateGroupGateway(groupUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a group gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupId} request group gateway uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_group_gateway(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupId = new pb_group.GroupId();
    groupId.setId(uuid_hex_to_base64(request.id));
    return client.deleteGroupGateway(groupId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Add a member to a group gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupGateway} request group gateway member: id, gateway_id
 * @returns {Promise<{}>} change response
 */
export async function add_group_gateway_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupDevice = new pb_group.GroupDevice();
    groupDevice.setId(uuid_hex_to_base64(request.id));
    groupDevice.setDeviceId(uuid_hex_to_base64(request.gateway_id));
    return client.addGroupGatewayMember(groupDevice, metadata(server))
        .then(response => response.toObject());
}

/**
 * Remove a member to a group gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GroupGateway} request group gateway member: id, gateway_id
 * @returns {Promise<{}>} change response
 */
export async function remove_group_gateway_member(server, request) {
    const client = new pb_group.GroupServicePromiseClient(server.address, null, null);
    const groupDevice = new pb_group.GroupDevice();
    groupDevice.setId(uuid_hex_to_base64(request.id));
    groupDevice.setDeviceId(uuid_hex_to_base64(request.gateway_id));
    return client.removeGroupGatewayMember(groupDevice, metadata(server))
        .then(response => response.toObject());
}
