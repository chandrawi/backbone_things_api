import { get_data_value, set_data_value } from '../common/type_value.js';
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
 * @property {?string} token
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
 * @param {*} r 
 * @returns {DeviceId}
 */
function get_device_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

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
 * @param {*} r 
 * @returns {TypeId}
 */
function get_type_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

/**
 * @typedef {Object} TypeSchema
 * @property {Uuid} id
 * @property {string} name
 * @property {string} description
 * @property {Uuid[]} models
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
        model_ids: r.modelIdsList.map((v) => {return base64_to_uuid_hex(v)})
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
 * @property {TypeSchema} device_type
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
        device_type: get_type_schema(r.deviceType),
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
 * @typedef {Object} DeviceCreate
 * @property {Uuid} id
 * @property {Uuid} gateway_id
 * @property {string} serial_number
 * @property {string} name
 * @property {string} description
 * @property {Uuid} type_id
 */

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
 * @param {*} r 
 * @returns {GatewayId}
 */
function get_gateway_id(r) {
    return {
        id: base64_to_uuid_hex(r.id)
    };
}

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
 * @property {TypeSchema} gateway_type
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
        gateway_type: get_type_schema(r.gatewayType),
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
 * @typedef {Object} GatewayCreate
 * @property {Uuid} id
 * @property {string} serial_number
 * @property {string} name
 * @property {string} description
 * @property {?Uuid} type_id
 */

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
 * @param {*} r 
 * @returns {ConfigId}
 */
function get_config_id(r) {
    return {
        id: r.id
    };
}

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
 * Read a device by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<DeviceSchema>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function read_device(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.readDevice(deviceId, metadata(server))
        .then(response => get_device_schema(response.toObject().result));
}

/**
 * Read a device by serial number
 * @param {ServerConfig} server server configuration: address, token
 * @param {SerialNumber} request serial number: serial_number
 * @returns {Promise<DeviceSchema>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function read_device_by_sn(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const serialNumber = new pb_device.SerialNumber();
    serialNumber.setSerialNumber(request.serial_number);
    return client.readDeviceBySn(serialNumber, metadata(server))
        .then(response => get_device_schema(response.toObject().result));
}

/**
 * Read devices by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceIds} request device uuid list: ids
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_ids(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceIds = new pb_device.DeviceIds();
    deviceIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listDeviceByIds(deviceIds, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceByGateway(gatewayId, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceByType(typeId, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceName} request device name: name
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_by_name(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceName = new pb_device.DeviceName();
    deviceName.setName(request.name);
    return client.listDeviceByName(deviceName, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Read devices with select options
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceOption} request device select option: gateway_id, type_id, name
 * @returns {Promise<DeviceSchema[]>} device schema: id, gateway_id, serial_number, name, description, device_type, configs
 */
export async function list_device_option(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceOption = new pb_device.DeviceOption();
    if (request.gateway_id) {
        deviceOption.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    }
    if (request.type_id) {
        deviceOption.setTypeId(uuid_hex_to_base64(request.type_id));
    }
    deviceOption.setName(request.name);
    return client.listDeviceOption(deviceOption, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceCreate} request device schema: id, gateway_id, serial_number, name, description, type_id
 * @returns {Promise<DeviceId>} device uuid: id
 */
export async function create_device(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeSchema = new pb_device.TypeSchema();
    typeSchema.setId(uuid_hex_to_base64(request.type_id));
    const deviceSchema = new pb_device.DeviceSchema();
    deviceSchema.setId(uuid_hex_to_base64(request.id));
    deviceSchema.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    deviceSchema.setSerialNumber(request.serial_number);
    deviceSchema.setName(request.name);
    deviceSchema.setDescription(request.description);
    deviceSchema.setDeviceType(typeSchema);
    return client.createDevice(deviceSchema, metadata(server))
        .then(response => get_device_id(response.toObject()));
}

/**
 * Update a device
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceUpdate} request device update: id, gateway_id, serial_number, name, description, type_id
 * @returns {Promise<{}>} update response
 */
export async function update_device(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceUpdate = new pb_device.DeviceUpdate();
    deviceUpdate.setId(uuid_hex_to_base64(request.id));
    if (request.gateway_id) {
        deviceUpdate.setGatewayId(uuid_hex_to_base64(request.gateway_id));
    }
    deviceUpdate.setSerialNumber(request.serial_number);
    deviceUpdate.setName(request.name);
    deviceUpdate.setDescription(request.description);
    deviceUpdate.setTypeId(request.type_id);
    return client.updateDevice(deviceUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a device
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_device(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.deleteDevice(deviceId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a gateway by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<GatewaySchema>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function read_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.readGateway(gatewayId, metadata(server))
        .then(response => get_gateway_schema(response.toObject().result));
}

/**
 * Read a gateway by serial number
 * @param {ServerConfig} server server configuration: address, token
 * @param {SerialNumber} request serial number: serial_number
 * @returns {Promise<GatewaySchema>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function read_gateway_by_sn(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const serialNumber = new pb_device.SerialNumber();
    serialNumber.setSerialNumber(request.serial_number);
    return client.readGatewayBySn(serialNumber, metadata(server))
        .then(response => get_gateway_schema(response.toObject().result));
}

/**
 * Read gateways by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayIds} request gateway uuid list: ids
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_ids(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayIds = new pb_device.GatewayIds();
    gatewayIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listGatewayByIds(gatewayIds, metadata(server))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways by type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.listGatewayByType(typeId, metadata(server))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayName} request gateway name: name
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_by_name(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayName = new pb_device.GatewayName();
    gatewayName.setName(request.name);
    return client.listGatewayByName(gatewayName, metadata(server))
        .then(response => get_gateway_schema_vec(response.toObject().resultsList));
}

/**
 * Read gateways with select options
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayOption} request gateway select option: type_id, name
 * @returns {Promise<GatewaySchema[]>} gateway schema: id, serial_number, name, description, gateway_type, configs
 */
export async function list_gateway_option(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayOption = new pb_device.GatewayOption();
    if (request.type_id) {
        gatewayOption.setTypeId(uuid_hex_to_base64(request.type_id));
    }
    gatewayOption.setName(request.name);
    return client.listGatewayOption(gatewayOption, metadata(server))
        .then(response => get_device_schema_vec(response.toObject().resultsList));
}

/**
 * Create a gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayCreate} request gateway schema: id, serial_number, name, description, type_id
 * @returns {Promise<GatewayId>} gateway uuid: id
 */
export async function create_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeSchema = new pb_device.TypeSchema();
    typeSchema.setId(uuid_hex_to_base64(request.type_id));
    const gatewaySchema = new pb_device.GatewaySchema();
    gatewaySchema.setId(uuid_hex_to_base64(request.id));
    gatewaySchema.setSerialNumber(request.serial_number);
    gatewaySchema.setName(request.name);
    gatewaySchema.setDescription(request.description);
    gatewaySchema.setGatewayType(typeSchema);
    return client.createGateway(gatewaySchema, metadata(server))
        .then(response => get_gateway_id(response.toObject()));
}

/**
 * Update a gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayUpdate} request gateway update: id, serial_number, name, description, type_id
 * @returns {Promise<{}>} update response 
 */
export async function update_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayUpdate = new pb_device.GatewayUpdate();
    gatewayUpdate.setId(uuid_hex_to_base64(request.id));
    gatewayUpdate.setSerialNumber(request.serial_number);
    gatewayUpdate.setName(request.name);
    gatewayUpdate.setDescription(request.description);
    gatewayUpdate.setTypeId(request.type_id);
    return client.updateGateway(gatewayUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a gateway
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.deleteGateway(gatewayId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a device configuration by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigId} request device config uuid: id
 * @returns {Promise<DeviceConfigSchema>} device config schema: id, device_id, name, value, category
 */
export async function read_device_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.readDeviceConfig(configId, metadata(server))
        .then(response => get_device_config_schema(response.toObject().result));
}

/**
 * Read device configurations by device uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceId} request device uuid: id
 * @returns {Promise<DeviceConfigSchema[]>} device config schema: id, device_id, name, value, category
 */
export async function list_device_config_by_device(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const deviceId = new pb_device.DeviceId();
    deviceId.setId(uuid_hex_to_base64(request.id));
    return client.listDeviceConfig(deviceId, metadata(server))
        .then(response => get_device_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {DeviceConfigSchema} request device config schema: device_id, name, value, category
 * @returns {Promise<ConfigId>} device config uuid: id
 */
export async function create_device_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configSchema = new pb_device.ConfigSchema();
    configSchema.setDeviceId(uuid_hex_to_base64(request.device_id));
    configSchema.setName(request.name);
    const value = set_data_value(request.value);
    configSchema.setConfigBytes(value.bytes);
    configSchema.setConfigType(value.type);
    configSchema.setCategory(request.category);
    return client.createDeviceConfig(configSchema, metadata(server))
        .then(response => get_config_id(response.toObject()));
}

/**
 * Update a device configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigUpdate} request device config update: id, name, value, category
 * @returns {Promise<{}>} update response
 */
export async function update_device_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configUpdate = new pb_device.ConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    const value = set_data_value(request.value);
    configUpdate.setConfigBytes(value.bytes);
    configUpdate.setConfigType(value.type);
    configUpdate.setCategory(request.category);
    return client.updateDeviceConfig(configUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a device configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigId} request device config uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_device_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.deleteDeviceConfig(configId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a gateway configuration by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigId} request gateway config uuid: id
 * @returns {Promise<GatewayConfigSchema>} gateway config schema: id, gateway_id, name, value, category
 */
export async function read_gateway_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.readGatewayConfig(configId, metadata(server))
        .then(response => get_gateway_config_schema(response.toObject().result));
}

/**
 * Read gateway configurations by gateway uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayId} request gateway uuid: id
 * @returns {Promise<GatewayConfigSchema[]>} gateway config schema: id, gateway_id, name, value, category
 */
export async function list_gateway_config_by_gateway(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const gatewayId = new pb_device.GatewayId();
    gatewayId.setId(uuid_hex_to_base64(request.id));
    return client.listGatewayConfig(gatewayId, metadata(server))
        .then(response => get_gateway_config_schema_vec(response.toObject().resultsList));
}

/**
 * Create a gateway configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {GatewayConfigSchema} request gateway config schema: gateway_id, name, value, category
 * @returns {Promise<ConfigId>} gateway config id: id
 */
export async function create_gateway_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configSchema = new pb_device.ConfigSchema();
    configSchema.setDeviceId(uuid_hex_to_base64(request.gateway_id));
    configSchema.setName(request.name);
    const value = set_data_value(request.value);
    configSchema.setConfigBytes(value.bytes);
    configSchema.setConfigType(value.type);
    configSchema.setCategory(request.category);
    return client.createGatewayConfig(configSchema, metadata(server))
        .then(response => get_config_id(response.toObject()));
}

/**
 * Update a gateway configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigUpdate} request gateway config update: id, name, value, category
 * @returns {Promise<{}>} update response
 */
export async function update_gateway_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configUpdate = new pb_device.ConfigUpdate();
    configUpdate.setId(request.id);
    configUpdate.setName(request.name);
    const value = set_data_value(request.value);
    configUpdate.setConfigBytes(value.bytes);
    configUpdate.setConfigType(value.type);
    configUpdate.setCategory(request.category);
    return client.updateGatewayConfig(configUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a gateway configuration
 * @param {ServerConfig} server server configuration: address, token
 * @param {ConfigId} request gateway config uuid: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_gateway_config(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const configId = new pb_device.ConfigId();
    configId.setId(request.id);
    return client.deleteGatewayConfig(configId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Read a device type by uuid
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeId} request type uuid: id
 * @returns {Promise<TypeSchema>} type schema: id, name, description, model_ids
 */
export async function read_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.readType(typeId, metadata(server))
        .then(response => get_type_schema(response.toObject().result));
}

/**
 * Read device types by uuid list
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeIds} request type uuid list: ids
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_by_ids(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeIds = new pb_device.TypeIds();
    typeIds.setIdsList(request.ids.map((id) => uuid_hex_to_base64(id)));
    return client.listTypeByIds(typeIds, metadata(server))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Read device types by name
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeName} request type name: name
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_by_name(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeName = new pb_device.TypeName();
    typeName.setName(request.name);
    return client.listTypeByName(typeName, metadata(server))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Read device types with select options
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeOption} request type select option: name
 * @returns {Promise<TypeSchema[]>} type schema: id, name, description, model_ids
 */
export async function list_type_option(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeOption = new pb_device.TypeOption();
    typeOption.setName(request.name);
    return client.listTypeOption(typeOption, metadata(server))
        .then(response => get_type_schema_vec(response.toObject().resultsList));
}

/**
 * Create a device type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeSchema} request type schema: id, name, description
 * @returns {Promise<TypeId>} type uuid: id
 */
export async function create_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeSchema = new pb_device.TypeSchema();
    typeSchema.setId(uuid_hex_to_base64(request.id));
    typeSchema.setName(request.name);
    typeSchema.setDescription(request.description);
    return client.createType(typeSchema, metadata(server))
        .then(response => get_type_id(response.toObject()));
}

/**
 * Update a device type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeUpdate} request type update: id, name, description
 * @returns {Promise<{}>} update response
 */
export async function update_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeUpdate = new pb_device.TypeUpdate();
    typeUpdate.setId(uuid_hex_to_base64(request.id));
    typeUpdate.setName(request.name);
    typeUpdate.setDescription(request.description);
    return client.updateType(typeUpdate, metadata(server))
        .then(response => response.toObject());
}

/**
 * Delete a device type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeId} request type id: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_type(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeId = new pb_device.TypeId();
    typeId.setId(uuid_hex_to_base64(request.id));
    return client.deleteType(typeId, metadata(server))
        .then(response => response.toObject());
}

/**
 * Add model to a device type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeModel} request type id: id, model_id
 * @returns {Promise<{}>} change response
 */
export async function add_type_model(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeModel = new pb_device.TypeModel();
    typeModel.setId(uuid_hex_to_base64(request.id));
    typeModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.addTypeModel(typeModel, metadata(server))
        .then(response => response.toObject());
}

/**
 * Remove model from a device type
 * @param {ServerConfig} server server configuration: address, token
 * @param {TypeModel} request type id: id, model_id
 * @returns {Promise<{}>} change response
 */
export async function remove_type_model(server, request) {
    const client = new pb_device.DeviceServicePromiseClient(server.address, null, null);
    const typeModel = new pb_device.TypeModel();
    typeModel.setId(uuid_hex_to_base64(request.id));
    typeModel.setModelId(uuid_hex_to_base64(request.model_id));
    return client.removeTypeModel(typeModel, metadata(server))
        .then(response => response.toObject());
}
