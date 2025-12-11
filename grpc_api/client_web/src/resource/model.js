import { get_data_value, set_data_value, get_data_type, set_data_type } from '../common/type_value.js';
import pb_model from '../proto/resource/model_grpc_web_pb.js';
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
 * @property {?string} access_token
 */

/**
 * @typedef {Object} ModelId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} ModelIds
 * @property {Uuid[]} ids
 */

/**
 * @param {*} r 
 * @returns {ModelId}
 */
function get_model_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} ModelName
 * @property {string} name
 */

/**
 * @typedef {Object} ModelCategory
 * @property {string} category
 */

/**
 * @typedef {Object} ModelOption
 * @property {?Uuid} type_id
 * @property {?string} name
 * @property {?string} category
 */

/**
 * @typedef {Object} TypeId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} ModelSchema
 * @property {Uuid} id
 * @property {string} category
 * @property {string} name
 * @property {string} description
 * @property {number[]|string[]} data_type
 * @property {ModelConfigSchema[][]} configs
 */

/**
 * @param {*} r 
 * @returns {ModelSchema}
 */
function get_model_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        category: r.category,
        name: r.name,
        description: r.description,
        data_type: r.dataTypeList.map((v) => { return get_data_type(v) }),
        configs: r.configsList.map((v) => { return get_model_config_schema_vec(v.configsList) })
    };
}

/**
 * @param {*} r 
 * @returns {ModelSchema[]}
 */
function get_model_schema_vec(r) {
    return r.map((v) => {return get_model_schema(v)});
}

/**
 * @typedef {Object} ModelUpdate
 * @property {Uuid} id
 * @property {?string} category
 * @property {?string} name
 * @property {?string} description
 * @property {?number[]|string[]} data_type
 */

/**
 * @typedef {Object} ModelConfigId
 * @property {number} id
 */

/**
 * @param {*} r 
 * @returns {ModelConfigId}
 */
function get_model_config_id(r) {
    return {
        id: r.id
    };
}

/**
 * @typedef {Object} ModelConfigSchema
 * @property {number} id
 * @property {Uuid} model_id
 * @property {number} index
 * @property {string} name
 * @property {number|bigint|string|Uint8Array|boolean} value
 * @property {string} category
 */

/**
 * @param {*} r 
 * @returns {ModelConfigSchema}
 */
function get_model_config_schema(r) {
    return {
        id: r.id,
        model_id: base64_to_uuid_hex(r.modelId),
        index: r.index,
        name: r.name,
        value: get_data_value(r.configBytes, r.configType),
        category: r.category
    };
}

/**
 * @param {*} r 
 * @returns {ModelConfigSchema[]}
 */
function get_model_config_schema_vec(r) {
    return r.map((v) => {return get_model_config_schema(v)});
}

/**
 * @typedef {Object} ModelConfigUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|bigint|string|Uint8Array|boolean} value
 * @property {?string} category
 */

/**
 * @typedef {Object} TagId
 * @property {Uuid} model_id
 * @property {number} tag
 */

/**
 * @typedef {Object} TagSchema
 * @property {Uuid} model_id
 * @property {number} tag
 * @property {string} name
 * @property {number[]} members
 */

/**
 * @param {*} r 
 * @returns {ModelConfigSchema}
 */
function get_tag_schema(r) {
    return {
        model_id: base64_to_uuid_hex(r.modelId),
        tag: r.tag,
        name: r.name,
        members: r.membersList
    };
}

/**
 * @typedef {Object} TagUpdate
 * @property {Uuid} model_id
 * @property {number} tag
 * @property {?string} name
 * @property {?number[]} members
 */


/**
 * Read a model by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelId} request model uuid: id
 * @returns {Promise<ModelSchema>} model schema: id, category, name, description, data_type, configs
 */
export async function read_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelId = new pb_model.ModelId();
    modelId.setId(uuid_hex_to_base64(request.id));
    return client.readModel(modelId, metadata(config.access_token))
        .then(response => get_model_schema(response.toObject().result));
}

/**
 * Read models by uuid list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelIds} request model uuid list: ids
 * @returns {Promise<ModelSchema[]>} model schema: id, category, name, description, data_type, configs
 */
export async function list_model_by_ids(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelIds = new pb_model.ModelIds();
    modelIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listModelByIds(modelIds, metadata(config.access_token))
        .then(response => get_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read models by name
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelName} request model name: name
 * @returns {Promise<ModelSchema[]>} model schema: id, category, name, description, data_type, configs
 */
export async function list_model_by_name(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelName = new pb_model.ModelName();
    modelName.setName(request.name);
    return client.listModelByName(modelName, metadata(config.access_token))
        .then(response => get_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read models by category
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelCategory} request model category: category
 * @returns {Promise<ModelSchema[]>} model schema: id, category, name, description, data_type, configs
 */
export async function list_model_by_category(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelCategory = new pb_model.ModelCategory();
    modelCategory.setCategory(request.category);
    return client.listModelByCategory(modelCategory, metadata(config.access_token))
        .then(response => get_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read models with select options
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelOption} request model select option: type_id, name, category
 * @returns {Promise<ModelSchema[]>} model schema: id, category, name, description, data_type, configs
 */
export async function list_model_option(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelOption = new pb_model.ModelOption();
    if (request.type_id) {
        modelOption.setTypeId(uuid_hex_to_base64(request.type_id));
    }
    modelOption.setName(request.name);
    modelOption.setCategory(request.category);
    return client.listModelOption(modelOption, metadata(config.access_token))
        .then(response => get_model_schema_vec(response.toObject().resultsList));
}

/**
 * Read models by type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<ModelSchema[]>} model schema: id, category, name, description, data_type, configs
 */
export async function list_model_by_type(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const typeId = new pb_model.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listModelByType(modelNameCategory, metadata(config.access_token))
        .then(response => get_model_schema_vec(response.toObject().resultsList));
}

/**
 * Create a model
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelSchema} request model schema: id, data_type, category, name, description
 * @returns {Promise<ModelId>} model id: id
 */
export async function create_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelSchema = new pb_model.ModelSchema();
    modelSchema.setId(uuid_hex_to_base64(request.id));
    modelSchema.setDataTypeList(request.data_type.map((v) => {return set_data_type(v)}));
    modelSchema.setCategory(request.category);
    modelSchema.setName(request.name);
    modelSchema.setDescription(request.description);
    return client.createModel(modelSchema, metadata(config.access_token))
        .then(response => get_model_id(response.toObject()));
}

/**
 * Update a model
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelUpdate} request model update: id, data_type, category, name, description
 * @returns {Promise<{}>} update response
 */
export async function update_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelUpdate = new pb_model.ModelUpdate();
    modelUpdate.setId(uuid_hex_to_base64(request.id));
    if (request.data_type) {
        modelUpdate.setDataTypeList(request.data_type.map((v) => {return set_data_type(v)}));
        modelUpdate.setDataTypeFlag(true);
    } else {
        modelUpdate.setDataTypeFlag(false);
    }
    modelUpdate.setCategory(request.category);
    modelUpdate.setName(request.name);
    modelUpdate.setDescription(request.description);
    return client.updateModel(modelUpdate, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Delete a model
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelId} request model uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelId = new pb_model.ModelId();
    modelId.setId(uuid_hex_to_base64(request.id));
    return client.deleteModel(modelId, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Read a model configuration by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelConfigId} request model config uuid: id
 * @returns {Promise<ModelConfigSchema>} model config schema: model_id, index, name, value, category
 */
export async function read_model_config(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const configId = new pb_model.ConfigId();
    configId.setId(request.id);
    return client.readModelConfig(configId, metadata(config.access_token))
        .then(response => get_model_config_schema(response.toObject().result));
}

/**
 * Read model configurations by model uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelId} request model uuid: id
 * @returns {Promise<ModelConfigSchema[]>} model config schema: model_id, index, name, value, category
 */
export async function list_model_config_by_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelId = new pb_model.ModelId();
    modelId.setId(uuid_hex_to_base64(request.id));
    return client.listModelConfig(modelId, metadata(config.access_token))
        .then(response => get_model_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a model configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelConfigSchema} request model config schema: model_id, index, name, value, category
 * @returns {Promise<ModelConfigId>} model config uuid: id
 */
export async function create_model_config(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const configSchema = new pb_model.ConfigSchema();
    configSchema.setModelId(uuid_hex_to_base64(request.model_id));
    configSchema.setIndex(request.index);
    configSchema.setName(request.name);
    const value = set_data_value(request.value);
    configSchema.setConfigBytes(value.bytes);
    configSchema.setConfigType(value.type);
    configSchema.setCategory(request.category);
    return client.createModelConfig(configSchema, metadata(config.access_token))
        .then(response => get_model_config_id(response.toObject()));
}

/**
 * Update a model configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelConfigUpdate} request model config update: id, name, value, category
 * @returns {Promise<{}>} update response 
 */
export async function update_model_config(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const configUpdate = new pb_model.ConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    const value = set_data_value(request.value);
    configUpdate.setConfigBytes(value.bytes);
    configUpdate.setConfigType(value.type);
    configUpdate.setCategory(request.category);
    return client.updateModelConfig(configUpdate, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Delete a model configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelConfigId} request model config uuid: id
 * @returns {Promise<{}>} delete response 
 */
export async function delete_model_config(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const configId = new pb_model.ConfigId();
    configId.setId(request.id);
    return client.deleteModelConfig(configId, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Read a tag by model uuid and tag number
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TagId} request tag id: model_id, tag
 * @returns {Promise<TagSchema>} tag schema: model_id, tag, name, members
 */
export async function read_tag(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const tagId = new pb_model.TagId();
    tagId.setModelId(uuid_hex_to_base64(request.model_id));
    tagId.setTag(request.tag);
    return client.readTag(tagId, metadata(config.access_token))
        .then(response => get_tag_schema(response.toObject().result));
}

/**
 * Read Tags by model uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ModelId} request model uuid: id
 * @returns {Promise<TagSchema[]>} tag schema: model_id, tag, name, members
 */
export async function list_tag_by_model(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const modelId = new pb_model.ModelId();
    modelId.setId(uuid_hex_to_base64(request.id));
    return client.listTagByModel(modelId, metadata(config.access_token))
        .then(response => get_tag_schema(response.toObject().resultsList));
}

/**
 * Create a tag
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TagSchema} request tag schema: model_id, tag, name, members
 * @returns {Promise<{}>} create response
 */
export async function create_tag(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const tagSchema = new pb_model.TagSchema();
    tagSchema.setModelId(uuid_hex_to_base64(request.model_id));
    tagSchema.setTag(request.tag);
    tagSchema.setName(request.name);
    tagSchema.setMembersList(request.members);
    return client.createTag(tagSchema, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Update a tag
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TagUpdate} request tag update: model_id, tag, name, members
 * @returns {Promise<{}>} update response 
 */
export async function update_tag(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const tagUpdate = new pb_model.TagUpdate();
    tagUpdate.setModelId(uuid_hex_to_base64(request.model_id));
    tagUpdate.setTag(request.tag);
    tagUpdate.setName(request.name);
    tagUpdate.setMembersList(request.members);
    return client.updateTag(tagUpdate, metadata(config.access_token))
        .then(response => response.toObject());
}

/**
 * Delete a tag
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TagId} request tag id: model_id, tag
 * @returns {Promise<{}>} delete response 
 */
export async function delete_tag(config, request) {
    const client = new pb_model.ModelServicePromiseClient(config.address, null, null);
    const tagId = new pb_model.TagId();
    tagId.setModelId(uuid_hex_to_base64(request.model_id));
    tagId.setTag(request.tag);
    return client.deleteTag(tagId, metadata(config.access_token))
        .then(response => response.toObject());
}
