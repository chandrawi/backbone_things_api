import { unpack_data_array, pack_data_array, pack_type } from '../common/type_value.js';
import { Tag } from '../common/tag.js';
import pb_data from '../proto/resource/data_grpc_web_pb.js';
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
 * @typedef {Object} DataTime
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataEarlier
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} earlier
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataLater
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataRange
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataNumber
 * @property {Uuid} device_id
 * @property {Uuid} model_id
 * @property {Date} timestamp
 * @property {number} number
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataSchema
 * @property {Uuid} model_id
 * @property {Uuid} device_id
 * @property {Date} timestamp
 * @property {(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataMultipleSchema
 * @property {Uuid[]} model_ids
 * @property {Uuid[]} device_ids
 * @property {Date[]} timestamps
 * @property {(number|bigint|string|Uint8Array|boolean)[][]} data
 * @property {?number[]} tags
 */

/**
 * @param {*} r 
 * @returns {DataSchema}
 */
function get_data_schema(r) {
    return {
        device_id: base64_to_uuid_hex(r.deviceId),
        model_id: base64_to_uuid_hex(r.modelId),
        timestamp: new Date(r.timestamp / 1000),
        data: unpack_data_array(r.dataBytes, r.dataTypeList),
        tag: r.tag ?? Tag.DEFAULT
    };
}

/**
 * @param {*} r 
 * @returns {DataSchema[]}
 */
function get_data_schema_vec(r) {
    return r.map((v) => {return get_data_schema(v)});
}

/**
 * @typedef {Object} DataGroupTime
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataGroupEarlier
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} earlier
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataGroupLater
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataGroupRange
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataGroupNumber
 * @property {Uuid[]} device_ids
 * @property {Uuid[]} model_ids
 * @property {Date} timestamp
 * @property {number} number
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataSetTime
 * @property {Uuid} set_id
 * @property {Date} timestamp
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataSetLater
 * @property {Uuid} set_id
 * @property {Date} later
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataSetRange
 * @property {Uuid} set_id
 * @property {Date} begin
 * @property {Date} end
 * @property {?number} tag
 */

/**
 * @typedef {Object} DataSetSchema
 * @property {Uuid} set_id
 * @property {Date} timestamp
 * @property {(number|bigint|string|Uint8Array|boolean)[]} data
 * @property {?number} tag
 */

/**
 * @param {*} r 
 * @returns {DataSetSchema}
 */
function get_data_set_schema(r) {
    return {
        set_id: base64_to_uuid_hex(r.setId),
        timestamp: new Date(r.timestamp / 1000),
        data: unpack_data_array(r.dataBytes, r.dataTypeList),
        tag: r.tag ?? Tag.DEFAULT
    };
}

/**
 * @param {*} r 
 * @returns {DataSetSchema[]}
 */
function get_data_set_schema_vec(r) {
    return r.map((v) => {return get_data_set_schema(v)});
}


/**
 * Read a data by time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataTime} request data time: device_id, model_id, timestamp, tag
 * @returns {Promise<DataSchema>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function read_data(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataTime = new pb_data.DataTime();
    dataTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataTime.setModelId(uuid_hex_to_base64(request.model_id));
    dataTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataTime.setTag(request.tag);
    return client.readData(dataTime, metadata(config.access_token))
        .then(response => get_data_schema(response.toObject().result));
}

/**
 * Read multiple data by specific time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataTime} request data time: device_id, model_id, timestamp, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_time(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataTime = new pb_data.DataTime();
    dataTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataTime.setModelId(uuid_hex_to_base64(request.model_id));
    dataTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataTime.setTag(request.tag);
    return client.listDataByTime(dataTime, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataEarlier} request data later: device_id, model_id, earlier, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataEarlier = new pb_data.DataEarlier();
    dataEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    dataEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataEarlier.setTag(request.tag);
    return client.listDataByEarlier(dataEarlier, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataLater} request data later: device_id, model_id, later, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataLater = new pb_data.DataLater();
    dataLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataLater.setModelId(uuid_hex_to_base64(request.model_id));
    dataLater.setLater(request.later.valueOf() * 1000);
    dataLater.setTag(request.tag);
    return client.listDataByLater(dataLater, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataRange} request data range: device_id, model_id, begin, end, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataRange = new pb_data.DataRange();
    dataRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataRange.setModelId(uuid_hex_to_base64(request.model_id));
    dataRange.setBegin(request.begin.valueOf() * 1000);
    dataRange.setEnd(request.end.valueOf() * 1000);
    dataRange.setTag(request.tag);
    return client.listDataByRange(dataRange, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by specific time and number before
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataNumber} request data time and number: device_id, model_id, timestamp, number, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_number_before(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataNumber = new pb_data.DataNumber();
    dataNumber.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataNumber.setModelId(uuid_hex_to_base64(request.model_id));
    dataNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    dataNumber.setNumber(request.number);
    dataNumber.setTag(request.tag);
    return client.listDataByNumberBefore(dataNumber, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by specific time and number after
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataNumber} request data time and number: device_id, model_id, timestamp, number, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_by_number_after(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataNumber = new pb_data.DataNumber();
    dataNumber.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataNumber.setModelId(uuid_hex_to_base64(request.model_id));
    dataNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    dataNumber.setNumber(request.number);
    dataNumber.setTag(request.tag);
    return client.listDataByNumberAfter(dataNumber, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and specific time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupTime} request data id list and time: device_ids, model_ids, timestamp, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_time(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsTime = new pb_data.DataGroupTime();
    dataIdsTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataIdsTime.setTag(request.tag);
    return client.listDataGroupByTime(dataIdsTime, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupEarlier} request data id list and later: device_ids, model_ids, earlier, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupEarlier = new pb_data.DataGroupEarlier();
    dataGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataGroupEarlier.setTag(request.tag);
    return client.listDataGroupByEarlier(dataGroupEarlier, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupLater} request data id list and later: device_ids, model_ids, later, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupLater = new pb_data.DataGroupLater();
    dataGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setLater(request.later.valueOf() * 1000);
    dataGroupLater.setTag(request.tag);
    return client.listDataGroupByLater(dataGroupLater, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupRange} request data id list and range: device_ids, model_ids, begin, end, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsRange = new pb_data.DataGroupRange();
    dataIdsRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setBegin(request.begin.valueOf() * 1000);
    dataIdsRange.setEnd(request.end.valueOf() * 1000);
    dataIdsRange.setTag(request.tag);
    return client.listDataGroupByRange(dataIdsRange, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and specific time and number before
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupNumber} request data id list, time and number: device_ids, model_ids, timestamp, number, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_number_before(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsNumber = new pb_data.DataGroupNumber();
    dataIdsNumber.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsNumber.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    dataIdsNumber.setNumber(request.number);
    dataIdsNumber.setTag(request.tag);
    return client.listDataGroupByNumberBefore(dataIdsNumber, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple data by uuid list and specific time and number after
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupNumber} request data id list, time and number: device_ids, model_ids, timestamp, number, tag
 * @returns {Promise<DataSchema[]>} data schema: device_id, model_id, timestamp, data, tag
 */
export async function list_data_group_by_number_after(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsNumber = new pb_data.DataGroupNumber();
    dataIdsNumber.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsNumber.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsNumber.setTimestamp(request.timestamp.valueOf() * 1000);
    dataIdsNumber.setNumber(request.number);
    dataIdsNumber.setTag(request.tag);
    return client.listDataGroupByNumberAfter(dataIdsNumber, metadata(config.access_token))
        .then(response => get_data_schema_vec(response.toObject().resultsList));
}

/**
 * Read a dataset by time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSetTime} request dataset time: set_id, timestamp, tag
 * @returns {Promise<DataSetSchema>} data set schema: set_id, timestamp, data, tag
 */
export async function read_data_set(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataSetTime = new pb_data.DataSetTime();
    dataSetTime.setSetId(uuid_hex_to_base64(request.set_id));
    dataSetTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataSetTime.setTag(request.tag);
    return client.readDataSet(dataSetTime, metadata(config.access_token))
        .then(response => get_data_set_schema(response.toObject().result));
}

/**
 * Read multiple dataset by specific time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSetTime} request dataset time: set_id, timestamp, tag
 * @returns {Promise<DataSetSchema[]>} data set schema: set_id, timestamp, data, tag
 */
export async function list_data_set_by_time(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const datasetTime = new pb_data.DataSetTime();
    datasetTime.setSetId(uuid_hex_to_base64(request.set_id));
    datasetTime.setTimestamp(request.timestamp.valueOf() * 1000);
    datasetTime.setTag(request.tag);
    return client.listDataSetByTime(datasetTime, metadata(config.access_token))
        .then(response => get_data_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple dataset by earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSetLater} request dataset later: set_id, earlier, tag
 * @returns {Promise<DataSetSchema[]>} data set schema: set_id, timestamp, data, tag
 */
export async function list_data_set_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataSetEarlier = new pb_data.DataSetLater();
    dataSetEarlier.setSetId(uuid_hex_to_base64(request.set_id));
    dataSetEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataSetEarlier.setTag(request.tag);
    return client.listDataSetByEarlier(dataSetLater, metadata(config.access_token))
        .then(response => get_data_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple dataset by later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSetLater} request dataset later: set_id, later, tag
 * @returns {Promise<DataSetSchema[]>} data set schema: set_id, timestamp, data, tag
 */
export async function list_data_set_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataSetLater = new pb_data.DataSetLater();
    dataSetLater.setSetId(uuid_hex_to_base64(request.set_id));
    dataSetLater.setLater(request.later.valueOf() * 1000);
    dataSetLater.setTag(request.tag);
    return client.listDataSetByLater(dataSetLater, metadata(config.access_token))
        .then(response => get_data_set_schema_vec(response.toObject().resultsList));
}

/**
 * Read multiple dataset by range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSetRange} request dataset range: set_id, begin, end, tag
 * @returns {Promise<DataSetSchema[]>} data set schema: set_id, timestamp, data, tag
 */
export async function list_data_set_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const datasetRange = new pb_data.DataSetRange();
    datasetRange.setSetId(uuid_hex_to_base64(request.set_id));
    datasetRange.setBegin(request.begin.valueOf() * 1000);
    datasetRange.setEnd(request.end.valueOf() * 1000);
    datasetRange.setTag(request.tag);
    return client.listDataSetByRange(datasetRange, metadata(config.access_token))
        .then(response => get_data_set_schema_vec(response.toObject().resultsList));
}

/**
 * Create a data
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataSchema} request data schema: device_id, model_id, timestamp, data, tag
 * @returns {Promise<null>} create response
 */
export async function create_data(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataSchema = new pb_data.DataSchema();
    dataSchema.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataSchema.setModelId(uuid_hex_to_base64(request.model_id));
    dataSchema.setTimestamp(request.timestamp.valueOf() * 1000);
    dataSchema.setDataBytes(pack_data_array(request.data));
    for (const value of request.data) {
        dataSchema.addDataType(pack_type(value));
    }
    dataSchema.setTag(request.tag ?? Tag.DEFAULT);
    return client.createData(dataSchema, metadata(config.access_token))
        .then(response => null);
}

/**
 * Create multiple data
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataMultipleSchema} request data multiple schema: device_ids, model_ids, timestamps, data, tags
 * @returns {Promise<null>} create response
 */
export async function create_data_multiple(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataMultiSchema = new pb_data.DataMultipleSchema();
    const number = request.device_ids.length;
    const tags = request.tags ?? Array(number).fill(Tag.DEFAULT);
    const lengths = [request.model_ids.length, request.timestamps.length, request.data.length, tags.length];
    if (lengths.some(length => length != number)) {
        throw new Error("INVALID_ARGUMENT");
    }
    for (let i=0; i<number; i++) {
        const dataSchema = new pb_data.DataSchema();
        dataSchema.setDeviceId(uuid_hex_to_base64(request.device_ids[i]));
        dataSchema.setModelId(uuid_hex_to_base64(request.model_ids[i]));
        dataSchema.setTimestamp(request.timestamps[i].valueOf() * 1000);
        dataSchema.setDataBytes(pack_data_array(request.data[i]));
        for (const value of request.data[i]) {
            dataSchema.addDataType(pack_type(value));
        }
        dataSchema.setTag(tags[i] ?? Tag.DEFAULT);
        dataMultiSchema.addSchemas(dataSchema);
    }
    return client.createDataMultiple(dataMultiSchema, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a data
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataTime} request data time: device_id, model_id, timestamp, tag
 * @returns {Promise<null>} delete response
 */
export async function delete_data(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataTime = new pb_data.DataTime();
    dataTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataTime.setModelId(uuid_hex_to_base64(request.model_id));
    dataTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataTime.setTag(request.tag);
    return client.deleteData(dataTime, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a data timestamp by id
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataTime} request data time: device_id, model_id, timestamp, tag
 * @returns {Promise<Date>} data timestamp
 */
export async function read_data_timestamp(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataTime = new pb_data.DataTime();
    dataTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataTime.setModelId(uuid_hex_to_base64(request.model_id));
    dataTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataTime.setTag(request.tag);
    return client.readDataTimestamp(dataTime, metadata(config.access_token))
        .then(response => new Date(response.toObject().timestamp / 1000));
}

/**
 * Read multiple data timestamp by earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataEarlier} request data later: device_id, model_id, earlier, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_timestamp_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataEarlier = new pb_data.DataEarlier();
    dataEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    dataEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataEarlier.setTag(request.tag);
    return client.listDataTimestampByEarlier(dataEarlier, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read multiple data timestamp by later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataLater} request data later: device_id, model_id, later, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_timestamp_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataLater = new pb_data.DataLater();
    dataLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataLater.setModelId(uuid_hex_to_base64(request.model_id));
    dataLater.setLater(request.later.valueOf() * 1000);
    dataLater.setTag(request.tag);
    return client.listDataTimestampByLater(dataLater, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read multiple data timestamp by range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataRange} request data range: device_id, model_id, begin, end, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_timestamp_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataRange = new pb_data.DataRange();
    dataRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataRange.setModelId(uuid_hex_to_base64(request.model_id));
    dataRange.setBegin(request.begin.valueOf() * 1000);
    dataRange.setEnd(request.end.valueOf() * 1000);
    dataRange.setTag(request.tag);
    return client.listDataTimestampByRange(dataRange, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read a data timestamp by uuid list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupTime} request data id list and time: device_ids, model_ids, timestamp, tag
 * @returns {Promise<Date>} data timestamp
 */
export async function read_data_group_timestamp(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupTime = new pb_data.DataGroupTime();
    dataGroupTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupTime.setTimestamp(request.timestamp.valueOf() * 1000);
    dataGroupTime.setTag(request.tag);
    return client.readDataGroupTimestamp(dataGroupTime, metadata(config.access_token))
        .then(response => new Date(response.toObject().timestamp / 1000));
}

/**
 * Read multiple data timestamp by uuid list and earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupEarlier} request data id list and later: device_ids, model_ids, earlier, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_group_timestamp_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupEarlier = new pb_data.DataGroupEarlier();
    dataGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataGroupEarlier.setTag(request.tag);
    return client.listDataGroupTimestampByEarlier(dataGroupEarlier, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read multiple data timestamp by uuid list and later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupLater} request data id list and later: device_ids, model_ids, later, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_group_timestamp_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupLater = new pb_data.DataGroupLater();
    dataGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setLater(request.later.valueOf() * 1000);
    dataGroupLater.setTag(request.tag);
    return client.listDataGroupTimestampByLater(dataGroupLater, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Read multiple data timestamp by uuid list and range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupRange} request data id list and range: device_ids, model_ids, begin, end, tag
 * @returns {Promise<Date[]>} data timestamp
 */
export async function list_data_group_timestamp_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsRange = new pb_data.DataGroupRange();
    dataIdsRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setBegin(request.begin.valueOf() * 1000);
    dataIdsRange.setEnd(request.end.valueOf() * 1000);
    dataIdsRange.setTag(request.tag);
    return client.listDataGroupTimestampByRange(dataIdsRange, metadata(config.access_token))
        .then(response => response.toObject().timestampsList.map((v) => new Date(v / 1000)));
}

/**
 * Count data
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataTime} request data count: device_id, model_id
 * @returns {Promise<number>} data count
 */
export async function count_data(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataTime = new pb_data.DataTime();
    dataTime.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataTime.setModelId(uuid_hex_to_base64(request.model_id));
    return client.countData(dataTime, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataEarlier} request data later: device_id, model_id, earlier, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataEarlier = new pb_data.DataEarlier();
    dataEarlier.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataEarlier.setModelId(uuid_hex_to_base64(request.model_id));
    dataEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataEarlier.setTag(request.tag);
    return client.countDataByEarlier(dataEarlier, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataLater} request data later: device_id, model_id, later, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataLater = new pb_data.DataLater();
    dataLater.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataLater.setModelId(uuid_hex_to_base64(request.model_id));
    dataLater.setLater(request.later.valueOf() * 1000);
    dataLater.setTag(request.tag);
    return client.countDataByLater(dataLater, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataRange} request data range: device_id, model_id, begin, end, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataRange = new pb_data.DataRange();
    dataRange.setDeviceId(uuid_hex_to_base64(request.device_id));
    dataRange.setModelId(uuid_hex_to_base64(request.model_id));
    dataRange.setBegin(request.begin.valueOf() * 1000);
    dataRange.setEnd(request.end.valueOf() * 1000);
    dataRange.setTag(request.tag);
    return client.countDataByRange(dataRange, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by id list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupTime} request data id list and time: device_ids, model_ids, timestamp, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_group(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsTime = new pb_data.DataGroupTime();
    dataIdsTime.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsTime.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsTime.setTag(request.tag);
    return client.countDataGroup(dataIdsTime, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by id list and earlier time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupEarlier} request data id list and later: device_ids, model_ids, earlier, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_group_by_earlier(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupEarlier = new pb_data.DataGroupEarlier();
    dataGroupEarlier.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupEarlier.setEarlier(request.earlier.valueOf() * 1000);
    dataGroupEarlier.setTag(request.tag);
    return client.countDataGroupByEarlier(dataGroupEarlier, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by id list and later time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupLater} request data id list and later: device_ids, model_ids, later, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_group_by_later(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataGroupLater = new pb_data.DataGroupLater();
    dataGroupLater.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataGroupLater.setLater(request.later.valueOf() * 1000);
    dataGroupLater.setTag(request.tag);
    return client.countDataGroupByLater(dataGroupLater, metadata(config.access_token))
        .then(response => response.toObject().count);
}

/**
 * Count data by id list and range time
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DataGroupRange} request data id list and range: device_ids, model_ids, begin, end, tag
 * @returns {Promise<number>} data count
 */
export async function count_data_group_by_range(config, request) {
    const client = new pb_data.DataServicePromiseClient(config.address, null, null);
    const dataIdsRange = new pb_data.DataGroupRange();
    dataIdsRange.setDeviceIdsList(request.device_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setModelIdsList(request.model_ids.map((id) => uuid_hex_to_base64(id)));
    dataIdsRange.setBegin(request.begin.valueOf() * 1000);
    dataIdsRange.setEnd(request.end.valueOf() * 1000);
    dataIdsRange.setTag(request.tag);
    return client.countDataGroupByRange(dataIdsRange, metadata(config.access_token))
        .then(response => response.toObject().count);
}
