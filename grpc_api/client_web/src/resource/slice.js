import pb_slice from '../proto/resource/slice_grpc_web_pb.js';
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
 * @typedef {Object} SliceId
 * @property {number} id
 */

/**
 * @typedef {Object} SliceIds
 * @property {number[]} ids
 */

/**
 * @typedef {Object} SliceTime
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 */

/**
 * @typedef {Object} SliceRange
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} begin
 * @property {Date} end
 */

/**
 * @typedef {Object} SliceNameTime
 * @property {string} name
 * @property {Date} timestamp
 */

/**
 * @typedef {Object} SliceNameRange
 * @property {string} name
 * @property {Date} begin
 * @property {Date} end
 */

/**
 * @typedef {Object} SliceOption
 * @property {?Uuid} device_id
 * @property {?Uuid} model_id
 * @property {?string} name
 * @property {?Date} begin_or_timestamp
 * @property {?Date} end
 */

/**
 * @typedef {Object} SliceGroupTime
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} timestamp
 */

/**
 * @typedef {Object} SliceGroupRange
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} begin
 * @property {Date} end
 */

/**
 * @typedef {Object} SliceGroupOption
 * @property {?Uuid[]} device_ids
 * @property {?Uuid[]} model_ids
 * @property {?string} name
 * @property {?Date} begin_or_timestamp
 * @property {?Date} end
 */

/**
 * @typedef {Object} SliceSchema
 * @property {number} id
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp_begin
 * @property {Date} timestamp_end
 * @property {string} name
 * @property {string} description
 */

/**
 * @param {*} r 
 * @returns {SliceSchema}
 */
function get_slice_schema(r) {
    return {
        id: r.id,
        device_id: base64_to_uuid_hex(r.deviceId),
        model_id: base64_to_uuid_hex(r.modelId),
        timestamp_begin: new Date(r.timestampBegin / 1000),
        timestamp_end: new Date(r.timestampEnd / 1000),
        name: r.name,
        description: r.description
    };
}

/**
 * @param {*} r 
 * @returns {SliceSchema[]}
 */
function get_slice_schema_vec(r) {
    return r.map((v) => {return get_slice_schema(v)});
}

/**
 * @typedef {Object} SliceUpdate
 * @property {number} id
 * @property {?Date} timestamp_begin
 * @property {?Date} timestamp_end
 * @property {?string} name
 * @property {?string} description
 */

/**
 * @typedef {Object} SliceSetTime
 * @property {Uuid} set_id
 * @property {Date} timestamp
 */

/**
 * @typedef {Object} SliceSetRange
 * @property {Uuid} set_id
 * @property {Date} begin
 * @property {Date} end
 */

/**
 * @typedef {Object} SliceSetOption
 * @property {Uuid} set_id
 * @property {?string} name
 * @property {?Date} begin_or_timestamp
 * @property {?Date} end
 */

/**
 * @typedef {Object} SliceSetSchema
 * @property {number} id
 * @property {Uuid} set_id
 * @property {Date} timestamp_begin
 * @property {Date} timestamp_end
 * @property {string} name
 * @property {string} description
 */

/**
 * @param {*} r 
 * @returns {SliceSetSchema}
 */
function get_slice_set_schema(r) {
    return {
        id: r.id,
        set_id: base64_to_uuid_hex(r.setId),
        timestamp_begin: new Date(r.timestampBegin / 1000),
        timestamp_end: new Date(r.timestampEnd / 1000),
        name: r.name,
        description: r.description
    };
}

/**
 * @param {*} r 
 * @returns {SliceSetSchema[]}
 */
function get_slice_set_schema_vec(r) {
    return r.map((v) => {return get_slice_set_schema(v)});
}


/**
 * Read a data slice by id
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceId} request data slice id: id
 * @returns {Promise<SliceSchema>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function read_slice(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceId = new pb_slice.SliceId();
    sliceId.setId(request.id);
    return client.readSlice(sliceId, metadata(config.access_token))
        .then(response => get_slice_schema(response.toObject().result));
}

/**
 * Read data slices by multiple id
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceIds} request data slice multiple id: ids
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_by_ids(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceIds = new pb_slice.SliceIds();
    sliceIds.setIdsList(request.ids);
    return client.listSliceByIds(sliceIds, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by specific time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceTime} request data slice time: device_id, model_id, timestamp
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_by_time(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceTime = new pb_slice.SliceTime();
    sliceTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    sliceTime.setModelId(uuid_hex_to_base64(request.model_id));
    sliceTime.setTimestamp(request.timestamp.valueOf() * 1000);
    return client.listSliceByTime(sliceTime, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by range time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceRange} request data slice range: device_id, model_id, begin, end
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_by_range(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceRange = new pb_slice.SliceRange();
    sliceRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    sliceRange.setModelId(uuid_hex_to_base64(request.model_id));
    sliceRange.setBegin(request.begin.valueOf() * 1000);
    sliceRange.setEnd(request.end.valueOf() * 1000);
    return client.listSliceByRange(sliceRange, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by name and specific time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceNameTime} request data slice name and time: name, timestamp
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_by_name_time(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceNameTime = new pb_slice.SliceNameTime();
    sliceNameTime.setName(request.name);
    sliceNameTime.setTimestamp(request.timestamp.valueOf() * 1000);
    return client.listSliceByNameTime(sliceNameTime, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by name and range time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceNameRange} request data slice name and range: name, begin, end
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_by_name_range(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceNameRange = new pb_slice.SliceNameRange();
    sliceNameRange.setName(request.name);
    sliceNameRange.setBegin(request.begin.valueOf() * 1000);
    sliceNameRange.setEnd(request.end.valueOf() * 1000);
    return client.listSliceByNameRange(sliceNameRange, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by options
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceOption} request data slice selection option: device_id, model_id, name, begin_or_timestamp, end
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_option(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceOption = new pb_slice.SliceOption();
    if (request.device_id) {
        sliceOption.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        sliceOption.setModelId(uuid_hex_to_base64(request.model_id));
    }
    sliceOption.setName(request.name);
    if (request.begin instanceof Date) {
        sliceOption.setBegin(request.timestamp_begin.valueOf() * 1000);
    }
    if (request.end instanceof Date) {
        sliceOption.setEnd(request.timestamp_end.valueOf() * 1000);
    }
    return client.listSliceOption(sliceOption, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by specific time and uuid list
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceGroupTime} request data slice group time: device_ids, model_ids, timestamp
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_group_by_time(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceGroupTime = new pb_slice.SliceGroupTime();
    sliceGroupTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    sliceGroupTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    sliceGroupTime.setTimestamp(request.timestamp.valueOf() * 1000);
    return client.listSliceGroupByTime(sliceGroupTime, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by range time and uuid list
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceGroupRange} request data slice group range: device_ids, model_ids, begin, end
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_group_by_range(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceGroupRange = new pb_slice.SliceGroupRange();
    sliceGroupRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    sliceGroupRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    sliceGroupRange.setBegin(request.begin.valueOf() * 1000);
    sliceGroupRange.setEnd(request.end.valueOf() * 1000);
    return client.listSliceGroupByRange(sliceGroupRange, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Read data slices by options and uuid list
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceGroupOption} request data slice selection option: device_ids, model_ids, name, begin_or_timestamp, end
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_group_option(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceGroupOption = new pb_slice.SliceGroupOption();
    if (request.device_ids) {
        sliceGroupOption.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        sliceGroupOption.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    sliceGroupOption.setName(request.name);
    if (request.begin instanceof Date) {
        sliceGroupOption.setBegin(request.timestamp_begin.valueOf() * 1000);
    }
    if (request.end instanceof Date) {
        sliceGroupOption.setEnd(request.timestamp_end.valueOf() * 1000);
    }
    return client.listSliceGroupOption(sliceGroupOption, metadata(config.access_token))
        .then(response => get_slice_schema_vec(response.toObject().resultsList));
}

/**
 * Create a data slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceSchema} request data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 * @returns {Promise<number>} data slice id
 */
export async function create_slice(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceSchema = new pb_slice.SliceSchema();
    sliceSchema.setDeviceId(uuid_hex_to_base64(request.device_id));
    sliceSchema.setModelId(uuid_hex_to_base64(request.model_id));
    sliceSchema.setTimestampBegin(request.timestamp_begin.valueOf() * 1000);
    sliceSchema.setTimestampEnd(request.timestamp_end.valueOf() * 1000);
    sliceSchema.setName(request.name);
    sliceSchema.setDescription(request.description);
    return client.createSlice(sliceSchema, metadata(config.access_token))
        .then(response => response.toObject().id);
}

/**
 * Update a data slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceUpdate} request data slice update: id, timestamp_begin, timestamp_end, name, description
 * @returns {Promise<null>} update response
 */
export async function update_slice(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceUpdate = new pb_slice.SliceUpdate();
    sliceUpdate.setId(request.id);
    if (request.timestamp_begin instanceof Date) {
        sliceUpdate.setTimestampBegin(request.timestamp_begin.valueOf() * 1000);
    }
    if (request.timestamp_end instanceof Date) {
        sliceUpdate.setTimestampEnd(request.timestamp_end.valueOf() * 1000);
    }
    sliceUpdate.setName(request.name);
    sliceUpdate.setDescription(request.description);
    return client.updateSlice(sliceUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a data slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceId} request data slice id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_slice(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceId = new pb_slice.SliceId();
    sliceId.setId(request.id);
    return client.deleteSlice(sliceId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a data set slice by id
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceId} request data set slice id: id
 * @returns {Promise<SliceSetSchema>} data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function read_slice_set(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceId = new pb_slice.SliceId();
    sliceId.setId(request.id);
    return client.readSliceSet(sliceId, metadata(config.access_token))
        .then(response => get_slice_set_schema(response.toObject().result));
}

/**
 * Read data set slices by multiple id
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceIds} request data slice multiple id: ids
 * @returns {Promise<SliceSchema[]>} data slice schema: device_id, model_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_by_ids(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceIds = new pb_slice.SliceIds();
    sliceIds.setIdsList(request.ids);
    return client.listSliceSetByIds(sliceIds, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data set slices by specific time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceSetTime} request data set slice time: set_id, timestamp
 * @returns {Promise<SliceSetSchema[]>} data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_by_time(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceTime = new pb_slice.SliceSetTime();
    sliceTime.setSetId(uuid_hex_to_base64(request.set_id));
    sliceTime.setTimestamp(request.timestamp.valueOf() * 1000);
    return client.listSliceSetByTime(sliceTime, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data set slices by range time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceSetRange} request data set slice range: set_id, begin, end
 * @returns {Promise<SliceSetSchema[]>} data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_by_range(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceRange = new pb_slice.SliceSetRange();
    sliceRange.setSetId(uuid_hex_to_base64(request.set_id));
    sliceRange.setBegin(request.begin.valueOf() * 1000);
    sliceRange.setEnd(request.end.valueOf() * 1000);
    return client.listSliceSetByRange(sliceRange, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data set slices by name and specific time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceNameTime} request data set slice name and time: name, timestamp
 * @returns {Promise<SliceSetSchema[]>} data slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_by_name_time(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceNameTime = new pb_slice.SliceNameTime();
    sliceNameTime.setName(request.name);
    sliceNameTime.setTimestamp(request.timestamp.valueOf() * 1000);
    return client.listSliceSetByNameTime(sliceNameTime, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data set slices by name and range time
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceNameRange} request data set slice name and range: name, begin, end
 * @returns {Promise<SliceSetSchema[]>} data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_by_name_range(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceNameRange = new pb_slice.SliceNameRange();
    sliceNameRange.setName(request.name);
    sliceNameRange.setBegin(request.begin.valueOf() * 1000);
    sliceNameRange.setEnd(request.end.valueOf() * 1000);
    return client.listSliceSetByNameRange(sliceNameRange, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data set slices by options
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceSetOption} request data set slice selection option: set_id, name, begin_or_timestamp, end
 * @returns {Promise<SliceSetSchema[]>} data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 */
export async function list_slice_set_option(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceOption = new pb_slice.SliceSetOption();
    if (request.set_id) {
        sliceOption.setSetId(uuid_hex_to_base64(request.set_id));
    }
    sliceOption.setName(request.name);
    if (request.begin instanceof Date) {
        sliceOption.setBegin(request.timestamp_begin.valueOf() * 1000);
    }
    if (request.end instanceof Date) {
        sliceOption.setEnd(request.timestamp_end.valueOf() * 1000);
    }
    return client.listSliceSetOption(sliceOption, metadata(config.access_token))
        .then(response => get_slice_set_schema_vec(response.toObject().resultsList));
}

/**
 * Create a data set slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceSetSchema} request data set slice schema: set_id, timestamp_begin, timestamp_end, name, description
 * @returns {Promise<number>} data set slice id
 */
export async function create_slice_set(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceSchema = new pb_slice.SliceSetSchema();
    sliceSchema.setSetId(uuid_hex_to_base64(request.set_id));
    sliceSchema.setTimestampBegin(request.timestamp_begin.valueOf() * 1000);
    sliceSchema.setTimestampEnd(request.timestamp_end.valueOf() * 1000);
    sliceSchema.setName(request.name);
    sliceSchema.setDescription(request.description);
    return client.createSliceSet(sliceSchema, metadata(config.access_token))
        .then(response => response.toObject().id);
}

/**
 * Update a data set slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceUpdate} request data set slice update: id, timestamp_begin, timestamp_end, name, description
 * @returns {Promise<null>} update response
 */
export async function update_slice_set(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceUpdate = new pb_slice.SliceUpdate();
    sliceUpdate.setId(request.id);
    if (request.timestamp_begin instanceof Date) {
        sliceUpdate.setTimestampBegin(request.timestamp_begin.valueOf() * 1000);
    }
    if (request.timestamp_end instanceof Date) {
        sliceUpdate.setTimestampEnd(request.timestamp_end.valueOf() * 1000);
    }
    sliceUpdate.setName(request.name);
    sliceUpdate.setDescription(request.description);
    return client.updateSliceSet(sliceUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a data set slice
 * @param {ServerConfig} config Resource server config Resource server config: address, access_token
 * @param {SliceId} request data set slice id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_slice_set(config, request) {
    const client = new pb_slice.SliceServicePromiseClient(config.address, null, null);
    const sliceId = new pb_slice.SliceId();
    sliceId.setId(request.id);
    return client.deleteSliceSet(sliceId, metadata(config.access_token))
        .then(response => null);
}
