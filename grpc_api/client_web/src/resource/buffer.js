import { get_data_values, set_data_values } from '../common/type_value.js';
import { Tag } from '../common/tag.js';
import pb_buffer from '../proto/resource/buffer_grpc_web_pb.js';
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
 * @typedef {Object} BufferId
 * @property {number} id
 */

/**
 * @param {*} r 
 * @returns {BufferId}
 */
function get_buffer_id(r) {
    return {
        id: r.id
    };
}

/**
 * @typedef {Object} BufferIds
 * @property {number[]} ids
 */

/**
 * @param {*} r 
 * @returns {BufferIds}
 */
function get_buffer_ids(r) {
    return {
        ids: r.idsList
    };
}

/**
 * @typedef {Object} BufferTime
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferEarlier
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} earlier
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferLater
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferRange
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferNumber
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {number} number
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSelector
 * @property {?Uuid} device_id
 * @property {?Uuid} model_id
 * @property {?number} tag
 */

/**
 * @typedef {Object} BuffersSelector
 * @property {number} number
 * @property {number} offset
 * @property {?Uuid} device_id
 * @property {?Uuid} model_id
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSchema
 * @property {number} id
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferMultipleSchema
 * @property {number[]} id
 * @property {Uuid[]} model_ids
 * @property {Uuid[]} device_ids
 * @property {Date[]} timestamps
 * @property {(number|bigint|string|Uint8Array|boolean)[][]} data
 * @property {?number[]} tags
 */

/**
 * @param {*} r 
 * @returns {BufferSchema}
 */
function get_buffer_schema(r) {
    return {
        id: r.id,
        device_id: base64_to_uuid_hex(r.deviceId),
        model_id: base64_to_uuid_hex(r.modelId),
        timestamp: new Date(r.timestamp / 1000),
        data: get_data_values(r.dataBytes, r.dataTypeList),
        tag: r.tag ?? Tag.DEFAULT
    };
}

/**
 * @param {*} r 
 * @returns {BufferSchema[]}
 */
function get_buffer_schema_vec(r) {
    return r.map((v) => {return get_buffer_schema(v)});
}

/**
 * @typedef {Object} BufferUpdate
 * @property {number} id
 * @property {?(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferUpdateTime
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {?(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupTime
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupEarlier
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} earlier
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupLater
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupRange
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupNumber
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} timestamp
 * @property {number} number
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferGroupSelector
 * @property {?Uuid[]} device_ids
 * @property {?Uuid[]} model_ids
 * @property {?number} tag
 */

/**
 * @typedef {Object} BuffersGroupSelector
 * @property {number} number
 * @property {number} offset
 * @property {?Uuid[]} device_ids
 * @property {?Uuid[]} model_ids
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSetTime
 * @property {Uuid} set_id
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSetLater
 * @property {Uuid} set_id
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSetRange
 * @property {Uuid} set_id
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} BufferSetSchema
 * @property {number[]} ids
 * @property {Uuid} set_id
 * @property {Date} timestamp
 * @property {(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @param {*} r 
 * @returns {BufferSetSchema}
 */
function get_buffer_set_schema(r) {
    return {
        ids: r.idsList,
        set_id: base64_to_uuid_hex(r.setId),
        timestamp: new Date(r.timestamp / 1000),
        data: get_data_values(r.dataBytes, r.dataTypeList),
        tag: r.tag ?? Tag.DEFAULT
    };
}

/**
 * @param {*} r 
 * @returns {BufferSetSchema[]}
 */
function get_buffer_set_schema_vec(r) {
    return r.map((v) => {return get_buffer_set_schema(v)});
}


/**
 * Read a data buffer by id
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferId} request data buffer id: id
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferId = new pb_buffer.BufferId();
    bufferId.setId(request.id);
    return client.readBuffer(bufferId, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read a data buffer by time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferTime} request data buffer time: device_id, model_id, timestamp, tag
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferTime = new pb_buffer.BufferTime();
    bufferTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferTime.setTag(request.tag);
    return client.readBufferByTime(bufferTime, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read data buffers by multiple id
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferIds} request data buffer id list: ids
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_ids(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferIds = new pb_buffer.BufferIds();
    bufferIds.setIdsList(request.ids);
    return client.listBufferByIds(bufferIds, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by specific time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferTime} request data buffer time: device_id, model_id, timestamp, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferTime = new pb_buffer.BufferTime();
    bufferTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferTime.setTag(request.tag);
    return client.listBufferByTime(bufferTime, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by earlier time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferEarlier} request data buffer earlier: device_id, model_id, earlier, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferEarlier = new pb_buffer.BufferEarlier();
    bufferEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    bufferEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferEarlier.setTag(request.tag);
    return client.listBufferByEarlier(bufferEarlier, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by later time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferLater} request data buffer later: device_id, model_id, later, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferLater = new pb_buffer.BufferLater();
    bufferLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferLater.setModelId(uuid_hex_to_base64(request.model_id));
    bufferLater.setLater(request.later.valueOf() * 1000);
    bufferLater.setTag(request.tag);
    return client.listBufferByLater(bufferLater, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by range time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferRange} request data buffer range: device_id, model_id, begin, end, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferRange = new pb_buffer.BufferRange();
    bufferRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferRange.setModelId(uuid_hex_to_base64(request.model_id));
    bufferRange.setBegin(request.begin.valueOf() * 1000);
    bufferRange.setEnd(request.end.valueOf() * 1000);
    bufferRange.setTag(request.tag);
    return client.listBufferByRange(bufferRange, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by specific time and number before
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferNumber} request data buffer time and number: device_id, model_id, timestamp, number, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_number_before(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferNumber = new pb_buffer.BufferNumber();
    bufferNumber.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferNumber.setModelId(uuid_hex_to_base64(request.model_id));
    bufferNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferNumber.setNumber(request.number);
    bufferNumber.setTag(request.tag);
    return client.listBufferByNumberBefore(bufferNumber, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by specific time and number after
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferNumber} request data buffer time and number: device_id, model_id, timestamp, number, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_by_number_after(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferNumber = new pb_buffer.BufferNumber();
    bufferNumber.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferNumber.setModelId(uuid_hex_to_base64(request.model_id));
    bufferNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferNumber.setNumber(request.number);
    bufferNumber.setTag(request.tag);
    return client.listBufferByNumberAfter(bufferNumber, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read first of a data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSelector} request data buffer selector: device_id, model_id, tag
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSelector = new pb_buffer.BufferSelector();
    if (request.device_id) {
        bufferSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        bufferSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    bufferSelector.setTag(request.tag);
    return client.readBufferFirst(bufferSelector, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read last of a data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSelector} request data buffer selector: device_id, model_id, tag
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSelector = new pb_buffer.BufferSelector();
    if (request.device_id) {
        bufferSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        bufferSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    bufferSelector.setTag(request.tag);
    return client.readBufferLast(bufferSelector, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read first of data buffers
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, device_id, model_id, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BuffersSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setTag(request.tag);
    buffersSelector.setNumber(request.number);
    return client.listBufferFirst(buffersSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read first of data buffers with offset
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, offset, device_id, model_id, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_first_offset(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BuffersSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setTag(request.tag);
    buffersSelector.setNumber(request.number);
    buffersSelector.setOffset(request.offset);
    return client.listBufferFirstOffset(buffersSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read last of data buffers
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, device_id, model_id, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BuffersSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setTag(request.tag);
    buffersSelector.setNumber(request.number);
    return client.listBufferLast(buffersSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read last of data buffers with offset
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, offset, device_id, model_id, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_last_offset(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BuffersSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setTag(request.tag);
    buffersSelector.setNumber(request.number);
    buffersSelector.setOffset(request.offset);
    return client.listBufferLastOffset(buffersSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list and specific time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupTime} request data buffer group and time: device_ids, model_ids, timestamp, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupTime = new pb_buffer.BufferGroupTime();
    bufferGroupTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferGroupTime.setTag(request.tag);
    return client.listBufferGroupByTime(bufferGroupTime, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list and earlier time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupEarlier} request data buffer group and earlier: device_ids, model_ids, earlier, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupEarlier = new pb_buffer.BufferGroupEarlier();
    bufferGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferGroupEarlier.setTag(request.tag);
    return client.listBufferGroupByEarlier(bufferGroupEarlier, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list and later time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupLater} request data buffer group and later: device_ids, model_ids, later, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupLater = new pb_buffer.BufferGroupLater();
    bufferGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setLater(request.later.valueOf() * 1000);
    bufferGroupLater.setTag(request.tag);
    return client.listBufferGroupByLater(bufferGroupLater, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list and range time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupRange} request data buffer group and range: device_ids, model_ids, begin, end, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupRange = new pb_buffer.BufferGroupRange();
    bufferGroupRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setBegin(request.begin.valueOf() * 1000);
    bufferGroupRange.setEnd(request.end.valueOf() * 1000);
    bufferGroupRange.setTag(request.tag);
    return client.listBufferGroupByRange(bufferGroupRange, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list, specific time and number before
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupNumber} request data buffer group time and number: device_ids, model_ids, timestamp, number, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_number_before(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupNumber = new pb_buffer.BufferGroupNumber();
    bufferGroupNumber.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupNumber.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferGroupNumber.setNumber(request.number);
    bufferGroupNumber.setTag(request.tag);
    return client.listBufferGroupByNumberBefore(bufferGroupNumber, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffers by uuid list, specific time and number after
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupNumber} request data buffer group time and number: device_ids, model_ids, timestamp, number, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_by_number_after(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupNumber = new pb_buffer.BufferGroupNumber();
    bufferGroupNumber.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupNumber.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferGroupNumber.setNumber(request.number);
    bufferGroupNumber.setTag(request.tag);
    return client.listBufferGroupByNumberAfter(bufferGroupNumber, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read first of a data buffer by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupSelector} request data buffer group selector: device_ids, model_ids, tag
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer_group_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupSelector = new pb_buffer.BufferGroupSelector();
    if (request.device_ids) {
        bufferGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        bufferGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    bufferGroupSelector.setTag(request.tag);
    return client.readBufferGroupFirst(bufferGroupSelector, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read last of a data buffer by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupSelector} request data buffer group selector: device_ids, model_ids, tag
 * @returns {Promise<BufferSchema>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function read_buffer_group_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupSelector = new pb_buffer.BufferGroupSelector();
    if (request.device_ids) {
        bufferGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        bufferGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    bufferGroupSelector.setTag(request.tag);
    return client.readBufferGroupLast(bufferGroupSelector, metadata(server))
        .then(response => get_buffer_schema(response.toObject().result));
}

/**
 * Read first of data buffers by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, device_ids, model_ids, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    return client.listBufferGroupFirst(buffersGroupSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read first of data buffers with offset by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, offset, device_ids, model_ids, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_first_offset(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    buffersGroupSelector.setOffset(request.offset);
    return client.listBufferGroupFirstOffset(buffersGroupSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read last of data buffers by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, device_ids, model_ids, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    return client.listBufferGroupLast(buffersGroupSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read last of data buffers with offset by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, offset, device_ids, model_ids, tag
 * @returns {Promise<BufferSchema[]>} data buffer schema: id, device_id, model_id, timestamp, data, tag
 */
export async function list_buffer_group_last_offset(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    buffersGroupSelector.setOffset(request.offset);
    return client.listBufferGroupLastOffset(buffersGroupSelector, metadata(server))
        .then(response => get_buffer_schema_vec(response.toObject().resultsList));
}

/**
 * Read a data buffer set by time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSetTime} request data buffer set time: set_id, timestamp, tag
 * @returns {Promise<BufferSetSchema>} data buffer set schema: ids, set_id, timestamp, data, tag
 */
export async function read_buffer_set(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSetTime = new pb_buffer.BufferSetTime();
    bufferSetTime.setSetId(uuid_hex_to_base64(request.set_id));
    bufferSetTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferSetTime.setTag(request.tag);
    return client.readBufferSet(bufferSetTime, metadata(server))
        .then(response => get_buffer_set_schema(response.toObject().result));
}

/**
 * Read data buffer sets by specific time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSetTime} request data buffer set time: set_id, timestamp, tag
 * @returns {Promise<BufferSetSchema[]>} data buffer set schema: ids, set_id, timestamp, data, tag
 */
export async function list_buffer_set_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSetTime = new pb_buffer.BufferSetTime();
    bufferSetTime.setSetId(uuid_hex_to_base64(request.set_id));
    bufferSetTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferSetTime.setTag(request.tag);
    return client.listBufferSetByTime(bufferSetTime, metadata(server))
        .then(response => get_buffer_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffer sets by earlier time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSetEarlier} request data buffer set earlier: set_id, earlier, tag
 * @returns {Promise<BufferSetSchema[]>} data buffer set schema: ids, set_id, timestamp, data, tag
 */
export async function list_buffer_set_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSetEarlier = new pb_buffer.BufferSetEarlier();
    bufferSetEarlier.setSetId(uuid_hex_to_base64(request.set_id));
    bufferSetEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferSetEarlier.setTag(request.tag);
    return client.listBufferSetByEarlier(bufferSetEarlier, metadata(server))
        .then(response => get_buffer_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffer sets by later time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSetLater} request data buffer set later: set_id, later, tag
 * @returns {Promise<BufferSetSchema[]>} data buffer set schema: ids, set_id, timestamp, data, tag
 */
export async function list_buffer_set_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSetLater = new pb_buffer.BufferSetLater();
    bufferSetLater.setSetId(uuid_hex_to_base64(request.set_id));
    bufferSetLater.setLater(request.later.valueOf() * 1000);
    bufferSetLater.setTag(request.tag);
    return client.listBufferSetByLater(bufferSetLater, metadata(server))
        .then(response => get_buffer_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read data buffer sets by range time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSetRange} request data buffer set range: set_id, begin, end, tag
 * @returns {Promise<BufferSetSchema[]>} data buffer set schema: ids, set_id, timestamp, data, tag
 */
export async function list_buffer_set_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSetRange = new pb_buffer.BufferSetRange();
    bufferSetRange.setSetId(uuid_hex_to_base64(request.set_id));
    bufferSetRange.setBegin(request.begin.valueOf() * 1000);
    bufferSetRange.setEnd(request.end.valueOf() * 1000);
    bufferSetRange.setTag(request.tag);
    return client.listBufferSetByRange(bufferSetRange, metadata(server))
        .then(response => get_buffer_set_schema_vec(response.toObject().resultsList));
}

/**
 * Create a data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferSchema} request data buffer schema: device_id, model_id, timestamp, data, tag
 * @returns {Promise<BufferId>} data buffer id: id
 */
export async function create_buffer(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferSchema = new pb_buffer.BufferSchema();
    bufferSchema.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferSchema.setModelId(uuid_hex_to_base64(request.model_id));
    bufferSchema.setTimestamp(request.timestamp.valueOf() * 1000);
    const value = set_data_values(request.data);
    bufferSchema.setDataBytes(value.bytes);
    for (const type of value.types) {
        bufferSchema.addDataType(type);
    }
    bufferSchema.setTag(request.tag ?? Tag.DEFAULT);
    return client.createBuffer(bufferSchema, metadata(server))
        .then(response => get_buffer_id(response.toObject()));
}

/**
 * Create multiple data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferMultipleSchema} request data buffer schema: device_ids, model_ids, timestamps, data, tags
 * @returns {Promise<BufferIds>} data buffer id: ids
 */
export async function create_buffer_multiple(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferMultiSchema = new pb_buffer.BufferMultipleSchema();
    const number = request.device_ids.length;
    const tags = request.tags ?? Array(number).fill(Tag.DEFAULT);
    const lengths = [request.model_ids.length, request.timestamps.length, request.data.length, tags.length];
    if (lengths.some(length => length != number)) {
        throw new Error("INVALID_ARGUMENT");
    }
    for (let i=0; i<number; i++) {
        const bufferSchema = new pb_buffer.BufferSchema();
        bufferSchema.setDeviceId(uuid_hex_to_base64(request.device_ids[i]));
        bufferSchema.setModelId(uuid_hex_to_base64(request.model_ids[i]));
        bufferSchema.setTimestamp(request.timestamps[i].valueOf() * 1000);
        const value = set_data_values(request.data[i]);
        bufferSchema.setDataBytes(value.bytes);
        for (const type of value.types) {
            bufferSchema.addDataType(type);
        }
        bufferSchema.setTag(tags[i] ?? Tag.DEFAULT);
        bufferMultiSchema.addSchemas(bufferSchema);
    }
    return client.createBufferMultiple(bufferMultiSchema, metadata(server))
        .then(response => get_buffer_ids(response.toObject()));
}

/**
 * Update a data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferUpdate} request data buffer update: id, data, tag
 * @returns {Promise<{}>} update response
 */
export async function update_buffer(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferUpdate = new pb_buffer.BufferUpdate();
    bufferUpdate.setId(request.id);
    if (typeof request.data == "object" && 'length' in request.data) {
        const value = set_data_values(request.data);
        bufferUpdate.setDataBytes(value.bytes);
        for (const type of value.types) {
            bufferUpdate.addDataType(type);
        }
    }
    bufferUpdate.setTag(request.tag);
    return client.updateBuffer(bufferUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Update a data buffer by time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferUpdateTime} request data buffer update: device_id, model_id, timestamp, data, tag
 * @returns {Promise<{}>} update response
 */
export async function update_buffer_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferUpdateTime = new pb_buffer.BufferUpdateTime();
    bufferUpdateTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferUpdateTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferUpdateTime.setTimestamp(request.timestamp.valueOf() * 1000);
    if (typeof request.data == "object" && 'length' in request.data) {
        const value = set_data_values(request.data);
        BufferUpdateTime.setDataBytes(value.bytes);
        for (const type of value.types) {
            BufferUpdateTime.addDataType(type);
        }
    }
    BufferUpdateTime.setTag(request.tag);
    return client.updateBufferByTime(BufferUpdateTime, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a data buffer
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferId} request data buffer id: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_buffer(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferId = new pb_buffer.BufferId();
    bufferId.setId(request.id);
    return client.deleteBuffer(bufferId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a data buffer by time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferTime} request data buffer time: device_id, model_id, timestamp, tag
 * @returns {Promise<{}>} delete response
 */
export async function delete_buffer_by_time(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferTime = new pb_buffer.BufferTime();
    bufferTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferTime.setTimestamp(request.timestamp.valueOf() * 1000);
    bufferTime.setTag(request.tag);
    return client.deleteBufferByTime(bufferTime, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a data buffer timestamp
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferTime} request data buffer time: device_id, model_id, tag
 * @returns {Promise<Date>} data buffer timestamp
 */
export async function read_buffer_timestamp(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferTime = new pb_buffer.BufferTime();
    bufferTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferTime.setTag(request.tag);
    return client.readBufferTimestamp(bufferTime, metadata(server))
        .then(response => new Date(response.toObject().timestamp / 1000));
}

/**
 * Read data buffers timestamp by earlier time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferEarlier} request data buffer earlier: device_id, model_id, earlier, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_timestamp_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferEarlier = new pb_buffer.BufferEarlier();
    bufferEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    bufferEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferEarlier.setTag(request.tag);
    return client.listBufferTimestampByEarlier(bufferEarlier, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read data buffers timestamp by later time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferLater} request data buffer later: device_id, model_id, later, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_timestamp_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferLater = new pb_buffer.BufferLater();
    bufferLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferLater.setModelId(uuid_hex_to_base64(request.model_id));
    bufferLater.setLater(request.later.valueOf() * 1000);
    bufferLater.setTag(request.tag);
    return client.listBufferTimestampByLater(bufferLater, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read data buffers timestamp by range time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferRange} request data buffer range: device_id, model_id, begin, end, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_timestamp_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferRange = new pb_buffer.BufferRange();
    bufferRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferRange.setModelId(uuid_hex_to_base64(request.model_id));
    bufferRange.setBegin(request.begin.valueOf() * 1000);
    bufferRange.setEnd(request.end.valueOf() * 1000);
    bufferRange.setTag(request.tag);
    return client.listBufferTimestampByRange(bufferRange, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read first of data buffers timestamp
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, device_id, model_id, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_timestamp_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BufferSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setNumber(request.number);
    buffersSelector.setTag(request.tag);
    return client.listBufferTimestampFirst(buffersSelector, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read last of data buffers timestamp
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersSelector} request data buffer selector: number, device_id, model_id, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_timestamp_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersSelector = new pb_buffer.BufferSelector();
    if (request.device_id) {
        buffersSelector.setDeviceId(uuid_hex_to_base64(request.device_id));
    }
    if (request.model_id) {
        buffersSelector.setModelId(uuid_hex_to_base64(request.model_id));
    }
    buffersSelector.setNumber(request.number);
    buffersSelector.setTag(request.tag);
    return client.listBufferTimestampLast(buffersSelector, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read a data buffer timestamp by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupTime} request data buffer group time: device_id, model_id, tag
 * @returns {Promise<Date>} data buffer timestamp
 */
export async function read_buffer_group_timestamp(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupTime = new pb_buffer.BufferGroupTime();
    bufferGroupTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setTag(request.tag);
    return client.readBufferGroupTimestamp(bufferGroupTime, metadata(server))
        .then(response => new Date(response.toObject().timestamp / 1000));
}

/**
 * Read data buffers timestamp by earlier time and uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupEarlier} request data buffer group earlier: device_id, model_id, earlier, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_group_timestamp_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupEarlier = new pb_buffer.BufferGroupEarlier();
    bufferGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferGroupEarlier.setTag(request.tag);
    return client.listBufferGroupTimestampByEarlier(bufferGroupEarlier, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read data buffers timestamp by later time and uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupLater} request data buffer group later: device_id, model_id, later, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_group_timestamp_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupLater = new pb_buffer.BufferGroupLater();
    bufferGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setLater(request.later.valueOf() * 1000);
    bufferGroupLater.setTag(request.tag);
    return client.listBufferGroupTimestampByLater(bufferGroupLater, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read data buffers timestamp by range time and uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupRange} request data buffer group range: device_id, model_id, begin, end, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_group_timestamp_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupRange = new pb_buffer.BufferGroupRange();
    bufferGroupRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setBegin(request.begin.valueOf() * 1000);
    bufferGroupRange.setEnd(request.end.valueOf() * 1000);
    bufferGroupRange.setTag(request.tag);
    return client.listBufferGroupTimestampByRange(bufferGroupRange, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read first of data buffers timestamp by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, device_ids, model_ids, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_group_timestamp_first(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    return client.listBufferGroupTimestampFirst(buffersGroupSelector, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read last of data buffers timestamp by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BuffersGroupSelector} request data buffer group selector: number, device_ids, model_ids, tag
 * @returns {Promise<Date[]>} data buffer timestamp
 */
export async function list_buffer_group_timestamp_last(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const buffersGroupSelector = new pb_buffer.BuffersGroupSelector();
    if (request.device_ids) {
        buffersGroupSelector.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    }
    if (request.model_ids) {
        buffersGroupSelector.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    }
    buffersGroupSelector.setTag(request.tag);
    buffersGroupSelector.setNumber(request.number);
    return client.listBufferGroupTimestampLast(buffersGroupSelector, metadata(server))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Count data buffers
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferTime} request data buffer time: device_id, model_id, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferTime = new pb_buffer.BufferTime();
    bufferTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferTime.setModelId(uuid_hex_to_base64(request.model_id));
    bufferTime.setTag(request.tag);
    return client.countBuffer(bufferTime, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by earlier time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferEarlier} request data buffer earlier: device_id, model_id, earlier, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferEarlier = new pb_buffer.BufferEarlier();
    bufferEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    bufferEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferEarlier.setTag(request.tag);
    return client.countBufferByEarlier(bufferEarlier, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by later time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferLater} request data buffer later: device_id, model_id, later, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferLater = new pb_buffer.BufferLater();
    bufferLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferLater.setModelId(uuid_hex_to_base64(request.model_id));
    bufferLater.setLater(request.later.valueOf() * 1000);
    bufferLater.setTag(request.tag);
    return client.countBufferByLater(bufferLater, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by range time
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferRange} request data buffer range: device_id, model_id, begin, end, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferRange = new pb_buffer.BufferRange();
    bufferRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    bufferRange.setModelId(uuid_hex_to_base64(request.model_id));
    bufferRange.setBegin(request.begin.valueOf() * 1000);
    bufferRange.setEnd(request.end.valueOf() * 1000);
    bufferRange.setTag(request.tag);
    return client.countBufferByRange(bufferRange, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by id list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupTime} request data buffer group time: device_ids, model_ids, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_group(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupTime = new pb_buffer.BufferGroupTime();
    bufferGroupTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupTime.setTag(request.tag);
    return client.countBufferGroup(bufferGroupTime, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by earlier time and id list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupEarlier} request data buffer group earlier: device_ids, model_ids, earlier, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_group_by_earlier(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupEarlier = new pb_buffer.BufferGroupEarlier();
    bufferGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    bufferGroupEarlier.setTag(request.tag);
    return client.countBufferGroupByEarlier(bufferGroupEarlier, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by later time and id list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupLater} request data buffer group later: device_ids, model_ids, later, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_group_by_later(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupLater = new pb_buffer.BufferGroupLater();
    bufferGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupLater.setLater(request.later.valueOf() * 1000);
    bufferGroupLater.setTag(request.tag);
    return client.countBufferGroupByLater(bufferGroupLater, metadata(server))
        .then(response => response.toObject().count);
}

/**
 * Count data buffers by range time and id list
 * @param {ServerConfig} server server configuration: address, token
 * @param {BufferGroupRange} request data buffer group range: device_ids, model_ids, begin, end, tag
 * @returns {Promise<number>} data buffer count
 */
export async function count_buffer_group_by_range(server, request) {
    const client = new pb_buffer.BufferServicePromiseClient(server.address, null, null);
    const bufferGroupRange = new pb_buffer.BufferGroupRange();
    bufferGroupRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    bufferGroupRange.setBegin(request.begin.valueOf() * 1000);
    bufferGroupRange.setEnd(request.end.valueOf() * 1000);
    bufferGroupRange.setTag(request.tag);
    return client.countBufferGroupByRange(bufferGroupRange, metadata(server))
        .then(response => response.toObject().count);
}
