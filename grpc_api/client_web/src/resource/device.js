import { get_data_value, set_data_value, get_data_type, set_data_type } from '../common/type_value.js';
import pb_device from '../proto/resource/device_grpc_web_pb.js';
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
 * @typedef {Object} DeviceId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} DeviceIds
 * @property {Uuid[]} ids
 */

/**
 * @typedef {Object} SerialNumber
 * @property {string} serial_number
 */

/**
 * @typedef {Object} TypeId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} TypeIds
 * @property {Uuid[]} ids
 */

/**
 * @typedef {Object} TypeName
 * @property {string} name
 */

/**
 * @typedef {Object} TypeOption
 * @property {?string} name
 */

/**
 * @typedef {Object} TypeSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} description
 * @property {Uuid[]} model_ids
 * @property {TypeConfigSchema[]} configs
 */

/**
 * @param {*} r 
 * @returns {TypeSchema}
 */
export function get_type_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        name: r.name,
        description: r.description,
        model_ids: r.modelIdsList.map((v) => {return base64_to_uuid_hex(v)}),
        configs: get_type_config_schema_vec(r.configsList)
    };
}

/**
 * @param {*} r 
 * @returns {TypeSchema[]}
 */
function get_type_schema_vec(r) {
    return r.map((v) => {return get_type_schema(v)});
}

/**
 * @typedef {Object} DeviceName
 * @property {string} name
 */

/**
 * @typedef {Object} DeviceOption
 * @property {?Uuid} gateway_id
 * @property {?Uuid} type_id
 * @property {?string} name
 */

/**
 * @typedef {Object} DeviceSchema
 * @property {Uuid} id
 * @property {Uuid} gateway_id
 * @property {string} serial_number
 * @property {string} name
 * @property {string} description
 * @property {Uuid} type_id
 * @property {string} type_name
 * @property {Uuid[]} model_ids
 * @property {DeviceConfigSchema[]} configs
 */

/**
 * @param {*} r 
 * @returns {DeviceSchema}
 */
function get_device_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        gateway_id: base64_to_uuid_hex(r.gatewayId),
        serial_number: r.serialNumber,
        name: r.name,
        description: r.description,
        type_id: r.typeId,
        type_name: r.typeName,
        model_ids: r.modelIdsList.map((v) => {return base64_to_uuid_hex(v)}),
        configs: get_device_config_schema_vec(r.configsList)
    };
}

/**
 * @param {*} r 
 * @returns {DeviceSchema[]}
 */
function get_device_schema_vec(r) {
    return r.map((v) => {return get_device_schema(v)});
}

/**
 * @typedef {Object} DeviceUpdate
 * @property {Uuid} id
 * @property {?Uuid} gateway_id
 * @property {?string} serial_number
 * @property {?string} name
 * @property {?string} description
 * @property {?Uuid} type_id
 */

/**
 * @typedef {Object} GatewayId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} GatewayIds
 * @property {Uuid[]} ids
 */

/**
 * @typedef {Object} GatewayName
 * @property {string} name
 */

/**
 * @typedef {Object} GatewayOption
 * @property {?Uuid} type_id
 * @property {?string} name
 */

/**
 * @typedef {Object} GatewaySchema
 * @property {Uuid} id
 * @property {string} serial_number
 * @property {string} name
 * @property {string} description
 * @property {Uuid} type_id
 * @property {string} type_name
 * @property {Uuid[]} model_ids
 * @property {GatewayConfigSchema[]} configs
 */

/**
 * @param {*} r 
 * @returns {GatewaySchema}
 */
function get_gateway_schema(r) {
    return {
        id: base64_to_uuid_hex(r.id),
        serial_number: r.serialNumber,
        name: r.name,
        description: r.description,
        type_id: r.typeId,
        type_name: r.typeName,
        model_ids: r.modelIdsList.map((v) => {return base64_to_uuid_hex(v)}),
        configs: get_device_config_schema_vec(r.configsList)
    };
}

/**
 * @param {*} r 
 * @returns {GatewaySchema[]}
 */
function get_gateway_schema_vec(r) {
    return r.map((v) => {return get_gateway_schema(v)});
}

/**
 * @typedef {Object} GatewayUpdate
 * @property {Uuid} id
 * @property {?string} serial_number
 * @property {?string} name
 * @property {?string} description
 * @property {?Uuid} type_id
 */

/**
 * @typedef {Object} TypeUpdate
 * @property {Uuid} id
 * @property {?string} name
 * @property {?string} description
 */

/**
 * @typedef {Object} TypeModel
 * @property {Uuid} id
 * @property {Uuid} model_id
 */

/**
 * @typedef {Object} ConfigId
 * @property {number} id
 */

/**
 * @typedef {Object} DeviceConfigSchema
 * @property {number} id
 * @property {Uuid} device_id
 * @property {string} name
 * @property {number|bigint|string|Uint8Array|boolean} value
 * @property {string} category
 */

/**
 * @typedef {Object} GatewayConfigSchema
 * @property {number} id
 * @property {Uuid} gateway_id
 * @property {string} name
 * @property {number|bigint|string|Uint8Array|boolean} value
 * @property {string} category
 */

/**
 * @param {*} r 
 * @returns {DeviceConfigSchema}
 */
function get_device_config_schema(r) {
    return {
        id: r.id,
        device_id: base64_to_uuid_hex(r.deviceId),
        name: r.name,
        value: get_data_value(r.configBytes, r.configType),
        category: r.category
    };
}

/**
 * @param {*} r 
 * @returns {DeviceConfigSchema[]}
 */
function get_device_config_schema_vec(r) {
    return r.map((v) => {return get_device_config_schema(v)});
}

/**
 * @param {*} r 
 * @returns {GatewayConfigSchema}
 */
function get_gateway_config_schema(r) {
    return {
        id: r.id,
        gateway_id: base64_to_uuid_hex(r.deviceId),
        name: r.name,
        value: get_data_value(r.configBytes, r.configType),
        category: r.category
    };
}

/**
 * @param {*} r 
 * @returns {GatewayConfigSchema[]}
 */
function get_gateway_config_schema_vec(r) {
    return r.map((v) => {return get_gateway_config_schema(v)});
}

/**
 * @typedef {Object} ConfigUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|bigint|string|Uint8Array|boolean} value
 * @property {?string} category
 */

/**
 * @typedef {Object} TypeConfigSchema
 * @property {number} id
 * @property {Uuid} type_id
 * @property {string} name
 * @property {number|string} value_type
 * @property {string} category
 */

/**
 * @typedef {Object} TypeConfigUpdate
 * @property {number} id
 * @property {?string} name
 * @property {?number|string} value_type
 * @property {?string} category
 */

/**
 * @param {*} r 
 * @returns {TypeConfigSchema}
 */
function get_type_config_schema(r) {
    return {
        id: r.id,
        type_id: base64_to_uuid_hex(r.typeId),
        name: r.name,
        value_type: get_data_type(r.configType),
        category: r.category
    };
}

/**
 * @param {*} r 
 * @returns {TypeConfigSchema[]}
 */
function get_type_config_schema_vec(r) {
    return r.map((v) => {return get_type_config_schema(v)});
}


/**
 * Read a device by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<DeviceSchema>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function read_device(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.readDevice(deviceId, metadata(config.access_token))
        .then(response => get_device_schema(response.toObject().result));
}

/**
 * Read a device by serial number
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {SerialNumber} request serial number: serial_number
 * @returns {Promise<DeviceSchema>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function read_device_by_sn(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const serialNumber = new pb_device.SerialNumber();
    serialNumber.setSerialNumber(request.serial_number);
    return client.readDeviceBySn(serialNumber, metadata(config.access_token))
        .then(response => get_device_schema(response.toObject().result));
}

/**
 * Read devices by uuid list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceIds} request device uuid list: ids
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_ids(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceIds = new pb_device.DeviceIds();
    deviceIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listDeviceByIds(deviceIds, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by gateway
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceByGateway(gatewayId, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceByType(typeId, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by name
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceName} request device name: name
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_name(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceName = new pb_device.DeviceName();
    deviceName.setName(request.name);
    return client.listDeviceByName(deviceName, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices with select options
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceOption} request device select option: gateway_id, type_id, name
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_option(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceOption = new pb_device.DeviceOption();
    if (request.gateway_id) {
        deviceOption.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    }
    if (request.type_id) {
        deviceOption.setTypeId(uuid_hex_to_base64(request.type_id));
    }
    deviceOption.setName(request.name);
    return client.listDeviceOption(deviceOption, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceSchema} request device schema: id, gateway_id, serial_number, name, description, type_id
 * @returns {Promise<Uuid>} device uuid
 */
export async function create_device(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceSchema = new pb_device.DeviceSchema();
    deviceSchema.setId(uuid_hex_to_base64(request.id));
    deviceSchema.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    deviceSchema.setSerialNumber(request.serial_number);
    deviceSchema.setName(request.name);
    deviceSchema.setDescription(request.description);
    deviceSchema.setTypeId(uuid_hex_to_base64(request.type_id));
    return client.createDevice(deviceSchema, metadata(config.access_token))
        .then(response => base64_to_uuid_hex(response.toObject().id));
}

/**
 * Update a device
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceUpdate} request device update: id, gateway_id, serial_number, name, description, type_id
 * @returns {Promise<null>} update response
 */
export async function update_device(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceUpdate = new pb_device.DeviceUpdate();
    deviceUpdate.setId(uuid_hex_to_base64(request.id));
    if (request.gateway_id) {
        deviceUpdate.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    }
    deviceUpdate.setSerialNumber(request.serial_number);
    deviceUpdate.setName(request.name);
    deviceUpdate.setDescription(request.description);
    deviceUpdate.setTypeId(request.type_id);
    return client.updateDevice(deviceUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a device
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<null>} delete response
 */
export async function delete_device(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.deleteDevice(deviceId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a gateway by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<GatewaySchema>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function read_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.readGateway(gatewayId, metadata(config.access_token))
        .then(response => get_gateway_schema(response.toObject().result));
}

/**
 * Read a gateway by serial number
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {SerialNumber} request serial number: serial_number
 * @returns {Promise<GatewaySchema>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function read_gateway_by_sn(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const serialNumber = new pb_device.SerialNumber();
    serialNumber.setSerialNumber(request.serial_number);
    return client.readGatewayBySn(serialNumber, metadata(config.access_token))
        .then(response => get_gateway_schema(response.toObject().result));
}

/**
 * Read gateways by uuid list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayIds} request gateway uuid list: ids
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_ids(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayIds = new pb_device.GatewayIds();
    gatewayIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listGatewayByIds(gatewayIds, metadata(config.access_token))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways by type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listGatewayByType(typeId, metadata(config.access_token))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways by name
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayName} request gateway name: name
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_name(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayName = new pb_device.GatewayName();
    gatewayName.setName(request.name);
    return client.listGatewayByName(gatewayName, metadata(config.access_token))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways with select options
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayOption} request gateway select option: type_id, name
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_option(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayOption = new pb_device.GatewayOption();
    if (request.type_id) {
        gatewayOption.setTypeId(uuid_hex_to_base64(request.type_id));
    }
    gatewayOption.setName(request.name);
    return client.listGatewayOption(gatewayOption, metadata(config.access_token))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Create a gateway
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewaySchema} request gateway schema: id, serial_number, name, description, type_id
 * @returns {Promise<Uuid>} gateway uuid
 */
export async function create_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewaySchema = new pb_device.GatewaySchema();
    gatewaySchema.setId(uuid_hex_to_base64(request.id));
    gatewaySchema.setSerialNumber(request.serial_number);
    gatewaySchema.setName(request.name);
    gatewaySchema.setDescription(request.description);
    gatewaySchema.setTypeId(uuid_hex_to_base64(request.type_id));
    return client.createGateway(gatewaySchema, metadata(config.access_token))
        .then(response => base64_to_uuid_hex(response.toObject().id));
}

/**
 * Update a gateway
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayUpdate} request gateway update: id, serial_number, name, description, type_id
 * @returns {Promise<null>} update response 
 */
export async function update_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayUpdate = new pb_device.GatewayUpdate();
    gatewayUpdate.setId(uuid_hex_to_base64(request.id));
    gatewayUpdate.setSerialNumber(request.serial_number);
    gatewayUpdate.setName(request.name);
    gatewayUpdate.setDescription(request.description);
    gatewayUpdate.setTypeId(request.type_id);
    return client.updateGateway(gatewayUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a gateway
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<null>} delete response
 */
export async function delete_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.deleteGateway(gatewayId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a device configuration by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request device config id: id
 * @returns {Promise<DeviceConfigSchema>} device config schema: id, device_id, name, value, category
 */
export async function read_device_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.readDeviceConfig(configId, metadata(config.access_token))
        .then(response => get_device_config_schema(response.toObject().result));
}

/**
 * Read device configurations by device uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<DeviceConfigSchema[]>} device config schema: id, device_id, name, value, category
 */
export async function list_device_config_by_device(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceConfig(deviceId, metadata(config.access_token))
        .then(response => get_device_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {DeviceConfigSchema} request device config schema: device_id, name, value, category
 * @returns {Promise<number>} device config id
 */
export async function create_device_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configSchema = new pb_device.ConfigSchema();
    configSchema.setDeviceId(uuid_hex_to_base64(request.device_id));
    configSchema.setName(request.name);
    const value = set_data_value(request.value);
    configSchema.setConfigBytes(value.bytes);
    configSchema.setConfigType(value.type);
    configSchema.setCategory(request.category);
    return client.createDeviceConfig(configSchema, metadata(config.access_token))
        .then(response => response.toObject().id);
}

/**
 * Update a device configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigUpdate} request device config update: id, name, value, category
 * @returns {Promise<null>} update response
 */
export async function update_device_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configUpdate = new pb_device.ConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    const value = set_data_value(request.value);
    configUpdate.setConfigBytes(value.bytes);
    configUpdate.setConfigType(value.type);
    configUpdate.setCategory(request.category);
    return client.updateDeviceConfig(configUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a device configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request device config id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_device_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.deleteDeviceConfig(configId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a gateway configuration by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request gateway config id: id
 * @returns {Promise<GatewayConfigSchema>} gateway config schema: id, gateway_id, name, value, category
 */
export async function read_gateway_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.readGatewayConfig(configId, metadata(config.access_token))
        .then(response => get_gateway_config_schema(response.toObject().result));
}

/**
 * Read gateway configurations by gateway uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<GatewayConfigSchema[]>} gateway config schema: id, gateway_id, name, value, category
 */
export async function list_gateway_config_by_gateway(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.listGatewayConfig(gatewayId, metadata(config.access_token))
        .then(response => get_gateway_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a gateway configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {GatewayConfigSchema} request gateway config schema: gateway_id, name, value, category
 * @returns {Promise<number>} gateway config id
 */
export async function create_gateway_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configSchema = new pb_device.ConfigSchema();
    configSchema.setDeviceId(uuid_hex_to_base64(request.gateway_id));
    configSchema.setName(request.name);
    const value = set_data_value(request.value);
    configSchema.setConfigBytes(value.bytes);
    configSchema.setConfigType(value.type);
    configSchema.setCategory(request.category);
    return client.createGatewayConfig(configSchema, metadata(config.access_token))
        .then(response => response.toObject().id);
}

/**
 * Update a gateway configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigUpdate} request gateway config update: id, name, value, category
 * @returns {Promise<null>} update response
 */
export async function update_gateway_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configUpdate = new pb_device.ConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    const value = set_data_value(request.value);
    configUpdate.setConfigBytes(value.bytes);
    configUpdate.setConfigType(value.type);
    configUpdate.setCategory(request.category);
    return client.updateGatewayConfig(configUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a gateway configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request gateway config id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_gateway_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.deleteGatewayConfig(configId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a device type by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<TypeSchema>} type schema: id, name, description, model_ids
 */
export async function read_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.readType(typeId, metadata(config.access_token))
        .then(response => get_type_schema(response.toObject().result));
}

/**
 * Read device types by uuid list
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeIds} request type uuid list: ids
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_by_ids(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeIds = new pb_device.TypeIds();
    typeIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listTypeByIds(typeIds, metadata(config.access_token))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Read device types by name
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeName} request type name: name
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_by_name(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeName = new pb_device.TypeName();
    typeName.setName(request.name);
    return client.listTypeByName(typeName, metadata(config.access_token))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Read device types with select options
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeOption} request type select option: name
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_option(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeOption = new pb_device.TypeOption();
    typeOption.setName(request.name);
    return client.listTypeOption(typeOption, metadata(config.access_token))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeSchema} request type schema: id, name, description
 * @returns {Promise<Uuid>} type uuid
 */
export async function create_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeSchema = new pb_device.TypeSchema();
    typeSchema.setId(uuid_hex_to_base64(request.id));
    typeSchema.setName(request.name);
    typeSchema.setDescription(request.description);
    return client.createType(typeSchema, metadata(config.access_token))
        .then(response => base64_to_uuid_hex(response.toObject().id));
}

/**
 * Update a device type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeUpdate} request type update: id, name, description
 * @returns {Promise<null>} update response
 */
export async function update_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeUpdate = new pb_device.TypeUpdate();
    typeUpdate.setId(uuid_hex_to_base64(request.id));
    typeUpdate.setName(request.name);
    typeUpdate.setDescription(request.description);
    return client.updateType(typeUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a device type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type id: id
 * @returns {Promise<null>} delete response
 */
export async function delete_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.deleteType(typeId, metadata(config.access_token))
        .then(response => null);
}

/**
 * Add model to a device type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeModel} request type id: id, model_id
 * @returns {Promise<null>} change response
 */
export async function add_type_model(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeModel = new pb_device.TypeModel();
    typeModel.setId(uuid_hex_to_base64(request.id));
    typeModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.addTypeModel(typeModel, metadata(config.access_token))
        .then(response => null);
}

/**
 * Remove model from a device type
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeModel} request type id: id, model_id
 * @returns {Promise<null>} change response
 */
export async function remove_type_model(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeModel = new pb_device.TypeModel();
    typeModel.setId(uuid_hex_to_base64(request.id));
    typeModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.removeTypeModel(typeModel, metadata(config.access_token))
        .then(response => null);
}

/**
 * Read a type configuration by uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request type config id: id
 * @returns {Promise<TypeConfigSchema>} type config schema: id, type_id, name, value, category
 */
export async function read_type_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.TypeConfigId();
    configId.setId(request.id);
    return client.readTypeConfig(configId, metadata(config.access_token))
        .then(response => get_type_config_schema(response.toObject().result));
}

/**
 * Read type configurations by type uuid
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<TypeConfigSchema[]>} type config schema: id, type_id, name, value, category
 */
export async function list_type_config_by_type(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listTypeConfig(typeId, metadata(config.access_token))
        .then(response => get_type_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a type configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeConfigSchema} request type config schema: type_id, name, type_value, category
 * @returns {Promise<number>} type config id
 */
export async function create_type_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configSchema = new pb_device.TypeConfigSchema();
    configSchema.setTypeId(uuid_hex_to_base64(request.type_id));
    configSchema.setName(request.name);
    configSchema.setConfigType(set_data_type(request.value_type));
    configSchema.setCategory(request.category);
    return client.createTypeConfig(configSchema, metadata(config.access_token))
        .then(response => response.toObject().id);
}

/**
 * Update a type configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {TypeConfigUpdate} request type config update: id, name, type_value, category
 * @returns {Promise<null>} update response
 */
export async function update_type_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configUpdate = new pb_device.TypeConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    configUpdate.setConfigType(set_data_type(request.value_type));
    configUpdate.setCategory(request.category);
    return client.updateTypeConfig(configUpdate, metadata(config.access_token))
        .then(response => null);
}

/**
 * Delete a type configuration
 * @param {ServerConfig} config Resource server config: address, access_token
 * @param {ConfigId} request type config uuid: id
 * @returns {Promise<null>} delete response
 */
export async function delete_type_config(config, request) {
    const client = new pb_device.DeviceServicePromiseClient(config.address, null, null);
    const configId = new pb_device.TypeConfigId();
    configId.setId(request.id);
    return client.deleteTypeConfig(configId, metadata(config.access_token))
        .then(response => null);
}
