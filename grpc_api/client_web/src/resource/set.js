import pb_set from '../proto/resource/set_grpc_web_pb.js';
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64,
    base64_to_bytes,
    bytes_to_base64
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
 * @typedef {Object} SetId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} SetIds
 * @property {Uuid[]} ids
 */

/**
 * @typedef {Object} SetName
 * @property {string} name
 */

/**
 * @typedef {Object} SetOption
 * @property {?Uuid} template_id
 * @property {?string} name
 */

/**
 * @param {*} r 
 * @returns {SetId}
 */
function get_set_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
* @typedef {Object} SetName
* @property {string} name
*/

/**
 * @typedef {Object} SetSchema
 * @property {Uuid} id
 * @property {Uuid} template_id
 * @property {string} name
 * @property {string} description
 * @property {SetMember[]} members
 */

/**
 * @param {*} r 
 * @returns {SetSchema}
 */
export function get_set_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        template_id: base64_to_uuid_hex(r.templateId),
        name: r.name,
        description: r.description,
        members: r.membersList.map((v) => {return get_set_member(v)})
    };
}

/**
 * @param {*} r 
 * @returns {SetSchema[]}
 */
function get_set_schema_vec(r) {
    return r.map((v) => {return get_set_schema(v)});
}

/**
 * @typedef {Object} SetMember
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {number[]} data_index
 */

/**
 * @param {*} r 
 * @returns {SetMember}
 */
export function get_set_member(r) {
    return {
        device_id: base64_to_uuid_hex(r.deviceId),
        model_id: base64_to_uuid_hex(r.modelId),
        data_index: base64_to_bytes(r.dataIndex)
    };
}

/**
 * @typedef {Object} SetUpdate
 * @property {Uuid} id
 * @property {?Uuid} template_id
 * @property {?string} name
 * @property {?string} description
 */

/**
 * @typedef {Object} SetMemberRequest
 * @property {Uuid} id
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {number[]} data_index
 */

/**
 * @typedef {Object} SetMemberSwap
 * @property {Uuid} id
 * @property {Uuid} device_id_1
 * @property {Uuid} model_id_1
 * @property {Uuid} device_id_2
 * @property {Uuid} model_id_2
 */

/**
 * @typedef {Object} SetTemplateId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} SetTemplateIds
 * @property {Uuid[]} ids
 */

/**
 * @typedef {Object} SetTemplateName
 * @property {string} name
 */

/**
 * @param {*} r 
 * @returns {SetTemplateId}
 */
function get_set_template_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
* @typedef {Object} SetTemplateName
* @property {string} name
*/

/**
 * @typedef {Object} SetTemplateOption
 * @property {?string} name
 */

/**
 * @typedef {Object} SetTemplateSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} description
 * @property {SetTemplateMember[]} members
 */

/**
 * @param {*} r 
 * @returns {SetTemplateSchema}
 */
export function get_set_template_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        description: r.description,
        members: r.membersList.map((v) => {return get_set_template_member(v)})
    };
}

/**
 * @param {*} r 
 * @returns {SetTemplateSchema[]}
 */
function get_set_template_schema_vec(r) {
    return r.map((v) => {return get_set_template_schema(v)});
}

/**
 * @typedef {Object} SetTemplateMember
 * @property {Uuid} type_id
 * @property {Uuid} model_id
 * @property {number[]} data_index
 */

/**
 * @param {*} r 
 * @returns {SetTemplateMember}
 */
export function get_set_template_member(r) {
    return {
        type_id: base64_to_uuid_hex(r.typeId),
        model_id: base64_to_uuid_hex(r.modelId),
        data_index: base64_to_bytes(r.dataIndex)
    };
}

/**
 * @typedef {Object} SetTemplateUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} description
 */

/**
 * @typedef {Object} SetTemplateMemberRequest
 * @property {Uuid} id
 * @property {Uuid} type_id
 * @property {Uuid} model_id
 * @property {number[]} data_index
 * @property {number} template_index
 */

/**
 * @typedef {Object} SetTemplateMemberSwap
 * @property {Uuid} id
 * @property {number} template_index_1
 * @property {number} template_index_2
 */


/**
 * Read a set by uuid
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetId} request set uuid: id
 * @returns {Promise<SetSchema>} set schema: id, template_id, name, description, members
 */
export async function read_set(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setId = new pb_set.SetId();
    setId.setId(uuid_hex_to_base64(request.id));
    return client.readSet(setId, metadata(config.access_token))
        .then(response => get_set_schema(response.toObject().result));
}

/**
 * Read sets by uuid list
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetIds} request set uuid list: ids
 * @returns {Promise<SetSchema[]>} set schema: id, template_id, name, description, members
 */
export async function list_set_by_ids(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setIds = new pb_set.SetIds();
    setIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listSetByIds(setIds, metadata(config.access_token))
        .then(response => get_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read sets by template uuid
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateId} request set template uuid: id
 * @returns {Promise<SetSchema[]>} set schema: id, template_id, name, description, members
 */
export async function list_set_by_template(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateId = new pb_set.SetTemplateId();
    templateId.setId(uuid_hex_to_base64(request.id));
    return client.listSetByTemplate(templateId, metadata(config.access_token))
        .then(response => get_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read sets by name
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetName} request set name: name
 * @returns {Promise<SetSchema[]>} set schema: id, template_id, name, description, members
 */
export async function list_set_by_name(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setName = new pb_set.SetName();
    setName.setName(request.name);
    return client.listSetByName(setName, metadata(config.access_token))
        .then(response => get_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read sets with select options
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetOption} request set option: template_id, name
 * @returns {Promise<SetSchema[]>} set schema: id, template_id, name, description, members
 */
export async function list_set_option(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setOption = new pb_set.SetOption();
    if (request.template_id) {
        setOption.setTemplateId(uuid_hex_to_base64(request.template_id));
    }
    setOption.setName(request.name);
    return client.listSetOption(setOption, metadata(config.access_token))
        .then(response => get_set_schema_vec(response.toObject().resultsList));
}

/**
 * Create a set
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetSchema} request set schema: id, template_id, name, description, members
 * @returns {Promise<SetId>} set uuid: id
 */
export async function create_set(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setSchema = new pb_set.SetSchema();
    setSchema.setId(uuid_hex_to_base64(request.id));
    setSchema.setTemplateId(uuid_hex_to_base64(request.template_id));
    setSchema.setName(request.name);
    setSchema.setDescription(request.description);
    return client.createSet(setSchema, metadata(config.access_token))
        .then(response => get_set_id(response.toObject()));
}

/**
 * Update a set
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetUpdate} request set update: id, template_id, name, description
 * @returns {Promise<{}>} update response
 */
export async function update_set(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setUpdate = new pb_set.SetUpdate();
    setUpdate.setId(uuid_hex_to_base64(request.id));
    setUpdate.setTemplateId(uuid_hex_to_base64(request.template_id));
    setUpdate.setName(request.name);
    setUpdate.setDescription(request.description);
    return client.updateSet(setUpdate, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Delete a set
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetId} request set uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_set(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setId = new pb_set.SetId();
    setId.setId(uuid_hex_to_base64(request.id));
    return client.deleteSet(setId, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Add a member to a set
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetMemberRequest} request set member request: id, device_id, model_id, data_index
 * @returns {Promise<{}>} change response
 */
export async function add_set_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setMember = new pb_set.SetMemberRequest();
    setMember.setId(uuid_hex_to_base64(request.id));
    setMember.setDeviceId(uuid_hex_to_base64(request.device_id));
    setMember.setModelId(uuid_hex_to_base64(request.model_id));
    setMember.setDataIndex(bytes_to_base64(request.data_index));
    return client.addSetMember(setMember, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Remove a member from a set
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetMemberRequest} request set member request: id, device_id, model_id, data_index
 * @returns {Promise<{}>} change response
 */
export async function remove_set_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setMember = new pb_set.SetMemberRequest();
    setMember.setId(uuid_hex_to_base64(request.id));
    setMember.setDeviceId(uuid_hex_to_base64(request.device_id));
    setMember.setModelId(uuid_hex_to_base64(request.model_id));
    return client.removeSetMember(setMember, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Swap a set member index position 
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetMemberSwap} request set member request: id, device_id_1, model_id_1, device_id_2, model_id_2
 * @returns {Promise<{}>} change response
 */
export async function swap_set_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setMember = new pb_set.SetMemberSwap();
    setMember.setId(uuid_hex_to_base64(request.id));
    setMember.setDeviceId1(uuid_hex_to_base64(request.device_id_1));
    setMember.setModelId1(uuid_hex_to_base64(request.model_id_1));
    setMember.setDeviceId2(uuid_hex_to_base64(request.device_id_2));
    setMember.setModelId2(uuid_hex_to_base64(request.model_id_2));
    return client.swapSetMember(setMember, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Read a set template by uuid
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateId} request set template uuid: id
 * @returns {Promise<SetTemplateSchema>} set template schema: id, name, description, members
 */
export async function read_set_template(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateId = new pb_set.SetTemplateId();
    templateId.setId(uuid_hex_to_base64(request.id));
    return client.readSetTemplate(templateId, metadata(config.access_token))
        .then(response => get_set_template_schema(response.toObject().result));
}

/**
 * Read set templates by uuid list
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateIds} request set template uuid list: ids
 * @returns {Promise<SetTemplateSchema[]>} set template schema: id, name, description, members
 */
export async function list_set_template_by_ids(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateIds = new pb_set.SetTemplateIds();
    templateIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listSetTemplateByIds(templateIds, metadata(config.access_token))
        .then(response => get_set_template_schema_vec(response.toObject().resultsList));
}

/**
 * Read set templates by name
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateName} request set template name: name
 * @returns {Promise<SetTemplateSchema[]>} set template schema: id, name, description, members
 */
export async function list_set_template_by_name(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateName = new pb_set.SetTemplateName();
    templateName.setName(request.name);
    return client.listSetTemplateByName(templateName, metadata(config.access_token))
        .then(response => get_set_template_schema_vec(response.toObject().resultsList));
}

/**
 * Read set templates with select options
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateOption} request set template option: name
 * @returns {Promise<SetTemplateSchema[]>} set schema: id, template_id, name, description, members
 */
export async function list_set_template_option(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const setOption = new pb_set.SetTemplateOption();
    setOption.setName(request.name);
    return client.listSetTemplateOption(setOption, metadata(config.access_token))
        .then(response => get_set_template_schema_vec(response.toObject().resultsList));
}

/**
 * Create a set template
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateSchema} request set template schema: id, name, description, members
 * @returns {Promise<SetTemplateId>} set template uuid: id
 */
export async function create_set_template(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateSchema = new pb_set.SetTemplateSchema();
    templateSchema.setId(uuid_hex_to_base64(request.id));
    templateSchema.setName(request.name);
    templateSchema.setDescription(request.description);
    return client.createSetTemplate(templateSchema, metadata(config.access_token))
        .then(response => get_set_template_id(response.toObject()));
}

/**
 * Update a set template
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateUpdate} request set template update: id, name, description
 * @returns {Promise<{}>} update response
 */
export async function update_set_template(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateUpdate = new pb_set.SetTemplateUpdate();
    templateUpdate.setId(uuid_hex_to_base64(request.id));
    templateUpdate.setName(request.name);
    templateUpdate.setDescription(request.description);
    return client.updateSetTemplate(templateUpdate, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Delete a set template
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateId} request set template uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_set_template(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateId = new pb_set.SetTemplateId();
    templateId.setId(uuid_hex_to_base64(request.id));
    return client.deleteSetTemplate(templateId, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Add a member to a set template
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateMemberRequest} request set member request: id, type_id, model_id, data_index
 * @returns {Promise<{}>} change response
 */
export async function add_set_template_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateMember = new pb_set.SetTemplateMemberRequest();
    templateMember.setId(uuid_hex_to_base64(request.id));
    templateMember.setTypeId(uuid_hex_to_base64(request.type_id));
    templateMember.setModelId(uuid_hex_to_base64(request.model_id));
    templateMember.setDataIndex(bytes_to_base64(request.data_index));
    return client.addSetTemplateMember(templateMember, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Remove a member from a set template
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateMemberRequest} request set member request: id, template_index
 * @returns {Promise<{}>} change response
 */
export async function remove_set_template_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateMember = new pb_set.SetTemplateMemberRequest();
    templateMember.setId(uuid_hex_to_base64(request.id));
    templateMember.setTemplateIndex(template_index);
    return client.removeSetTemplateMember(templateMember, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Swap a set template member index position 
 * @param {ServerConfig} config Resource server config Resource server config Resource server config: address, access_token
 * @param {SetTemplateMemberSwap} request set template member swap: id, template_index_1, template_index_2
 * @returns {Promise<{}>} change response
 */
export async function swap_set_template_member(config, request) {
    const client = new pb_set.SetServicePromiseClient(config.address, null, null);
    const templateMember = new pb_set.SetTemplateMemberSwap();
    templateMember.setId(uuid_hex_to_base64(request.id));
    templateMember.setTemplateIndex1(uuid_hex_to_base64(request.template_index_1));
    templateMember.setTemplateIndex2(uuid_hex_to_base64(request.template_index_2));
    return client.swapSetTemplateMember(templateMember, metadata(config.access_token))
        .then(response => response.toObject());
}
