import pb_config from "../proto/resource/config_grpc_web_pb.js";
import pb_auth from "../proto/auth/auth_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64,
    importKey,
    encryptMessage
} from "../common/utility.js";


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * Resource server configuration.
 * @param {String} address - Resource server address
 * @param {?String} auth_address - Authorization/Authentication server address
 * @param {?Uuid} api_id - Connected Resource API ID
 * @param {?Uuid} user_id - ID of the user connected to the server
 * @param {?String} auth_token - User identification token. Used for logout
 * @param {?String} access_token - Token containing user credential. Used for accessing resource
 * @param {?String} refresh_token - Token for refresh access_token expired time
 */
export class ResourceConfig {

    constructor(address, api_id, auth_token, access_token, refresh_token) {
        this.address = address;
        this.api_id = api_id;
        this.auth_token = auth_token;
        this.access_token = access_token;
        this.refresh_token = refresh_token;
    }

    /**
     * Login user to Authorization/Authentication server for accesing resource
     * @param {String} auth_address - Authorization/Authentication server address
     * @param {String} username
     * @param {String} password
     */
    async login(auth_address, username, password) {
        this.auth_address = auth_address;
        // get resource api address from resource server
        const client = new pb_config.ConfigServicePromiseClient(this.address, null, null);
        const apiIdRequest = new pb_config.ApiIdRequest();
        const api_id = await client.apiId(apiIdRequest, metadata(this.access_token))
            .then(response => base64_to_uuid_hex(response.toObject().apiId));
        this.api_id = api_id;
        // login user to auth server
        const client_auth = new pb_auth.AuthServicePromiseClient(auth_address, null, null);
        const userKeyRequest = new pb_auth.UserKeyRequest();
        const key = await client_auth.userPasswordKey(userKeyRequest)
            .then(response => response.toObject().publicKey);
        const userLoginRequest = new pb_auth.UserLoginRequest();
        userLoginRequest.setUsername(username);
        const pubkey = await importKey(key);
        const ciphertext = await encryptMessage(password, pubkey);
        userLoginRequest.setPassword(ciphertext);
        const login = await client_auth.userLogin(userLoginRequest)
            .then(response => response.toObject());
        this.auth_token = login.authToken;
        for (const map of login.accessTokensList) {
            if (map.api_id == self._api_id || map.api_id == 'ffffffff-ffff-ffff-ffff-ffffffffffff') {
                this.access_token = map.accessToken;
                this.refresh_token = map.refreshToken;
            }
        }
        this.user_id = base64_to_uuid_hex(login.userId);
    }

    /**
     * Refresh access token
     */
    async refresh() {
        if (this.auth_address) {
            const client = new pb_auth.AuthServicePromiseClient(this.auth_address, null, null);
            const userRefreshRequest = new pb_auth.UserRefreshRequest();
            userRefreshRequest.setApiId(uuid_hex_to_base64(this.api_id));
            userRefreshRequest.setAccessToken(this.access_token);
            userRefreshRequest.setRefreshToken(this.refresh_token);
            const refresh = await client.userRefresh(userRefreshRequest)
                .then(response => response.toObject());
            this.access_token = refresh.accessToken;
            this.refresh_token = refresh.refreshToken;
        }
    }

    /**
     * Logout user from Authorization/Authentication server
     */
    async logout() {
        if (this.auth_address && this.user_id) {
            const client = new pb_auth.AuthServicePromiseClient(this.auth_address, null, null);
            const userLogoutRequest = new pb_auth.UserLogoutRequest();
            userLogoutRequest.setUserId(uuid_hex_to_base64(this.user_id));
            userLogoutRequest.setAuthToken(this.auth_token);
            await client.userLogout(userLogoutRequest)
                .then(response => response.toObject());
            this.user_id = undefined;
            this.auth_token = undefined;
            this.access_token = undefined;
            this.refresh_token = undefined;
        }
    }

}


export {
    api_id,
    procedure_access,
    role_access
} from './config.js'
export {
    read_model,
    list_model_by_ids,
    list_model_by_name,
    list_model_by_category,
    list_model_option,
    list_model_by_type,
    create_model,
    update_model,
    delete_model,
    read_model_config,
    list_model_config_by_model,
    create_model_config,
    update_model_config,
    delete_model_config,
    read_tag,
    list_tag_by_model,
    create_tag,
    update_tag,
    delete_tag
} from './model.js';
export {
    read_device,
    read_device_by_sn,
    list_device_by_ids,
    list_device_by_gateway,
    list_device_by_type,
    list_device_by_name,
    list_device_option,
    create_device,
    update_device,
    delete_device,
    read_gateway,
    read_gateway_by_sn,
    list_gateway_by_ids,
    list_gateway_by_type,
    list_gateway_by_name,
    list_gateway_option,
    create_gateway,
    update_gateway,
    delete_gateway,
    read_device_config,
    list_device_config_by_device,
    create_device_config,
    update_device_config,
    delete_device_config,
    read_gateway_config,
    list_gateway_config_by_gateway,
    create_gateway_config,
    update_gateway_config,
    delete_gateway_config,
    read_type,
    list_type_by_ids,
    list_type_by_name,
    list_type_option,
    create_type,
    update_type,
    delete_type,
    add_type_model,
    remove_type_model,
    read_type_config,
    list_type_config_by_type,
    create_type_config,
    update_type_config,
    delete_type_config
} from './device.js';
export {
    read_group_model,
    list_group_model_by_ids,
    list_group_model_by_name,
    list_group_model_by_category,
    list_group_model_option,
    create_group_model,
    update_group_model,
    delete_group_model,
    add_group_model_member,
    remove_group_model_member,
    read_group_device,
    list_group_device_by_ids,
    list_group_device_by_name,
    list_group_device_by_category,
    list_group_device_option,
    create_group_device,
    update_group_device,
    delete_group_device,
    add_group_device_member,
    remove_group_device_member,
    read_group_gateway,
    list_group_gateway_by_ids,
    list_group_gateway_by_name,
    list_group_gateway_by_category,
    list_group_gateway_option,
    create_group_gateway,
    update_group_gateway,
    delete_group_gateway,
    add_group_gateway_member,
    remove_group_gateway_member
} from './group.js';
export {
    read_set,
    list_set_by_ids,
    list_set_by_template,
    list_set_by_name,
    list_set_option,
    create_set,
    update_set,
    delete_set,
    add_set_member,
    remove_set_member,
    swap_set_member,
    read_set_template,
    list_set_template_by_ids,
    list_set_template_by_name,
    list_set_template_option,
    create_set_template,
    update_set_template,
    delete_set_template,
    add_set_template_member,
    remove_set_template_member,
    swap_set_template_member
} from './set.js';
export {
    read_data,
    list_data_by_time,
    list_data_by_earlier,
    list_data_by_later,
    list_data_by_range,
    list_data_by_number_before,
    list_data_by_number_after,
    list_data_group_by_time,
    list_data_group_by_earlier,
    list_data_group_by_later,
    list_data_group_by_range,
    list_data_group_by_number_before,
    list_data_group_by_number_after,
    read_data_set,
    list_data_set_by_time,
    list_data_set_by_earlier,
    list_data_set_by_later,
    list_data_set_by_range,
    create_data,
    create_data_multiple,
    delete_data,
    read_data_timestamp,
    list_data_timestamp_by_earlier,
    list_data_timestamp_by_later,
    list_data_timestamp_by_range,
    read_data_group_timestamp,
    list_data_group_timestamp_by_earlier,
    list_data_group_timestamp_by_later,
    list_data_group_timestamp_by_range,
    count_data,
    count_data_by_earlier,
    count_data_by_later,
    count_data_by_range,
    count_data_group,
    count_data_group_by_earlier,
    count_data_group_by_later,
    count_data_group_by_range
} from './data.js';
export {
    read_buffer,
    read_buffer_by_time,
    list_buffer_by_ids,
    list_buffer_by_time,
    list_buffer_by_earlier,
    list_buffer_by_later,
    list_buffer_by_range,
    list_buffer_by_number_before,
    list_buffer_by_number_after,
    read_buffer_first,
    read_buffer_last,
    list_buffer_first,
    list_buffer_first_offset,
    list_buffer_last,
    list_buffer_last_offset,
    list_buffer_group_by_time,
    list_buffer_group_by_earlier,
    list_buffer_group_by_later,
    list_buffer_group_by_range,
    list_buffer_group_by_number_before,
    list_buffer_group_by_number_after,
    read_buffer_group_first,
    read_buffer_group_last,
    list_buffer_group_first,
    list_buffer_group_first_offset,
    list_buffer_group_last,
    list_buffer_group_last_offset,
    read_buffer_set,
    list_buffer_set_by_time,
    list_buffer_set_by_earlier,
    list_buffer_set_by_later,
    list_buffer_set_by_range,
    create_buffer,
    create_buffer_multiple,
    update_buffer,
    update_buffer_by_time,
    delete_buffer,
    delete_buffer_by_time,
    read_buffer_timestamp,
    list_buffer_timestamp_by_earlier,
    list_buffer_timestamp_by_later,
    list_buffer_timestamp_by_range,
    list_buffer_timestamp_first,
    list_buffer_timestamp_last,
    read_buffer_group_timestamp,
    list_buffer_group_timestamp_by_earlier,
    list_buffer_group_timestamp_by_later,
    list_buffer_group_timestamp_by_range,
    list_buffer_group_timestamp_first,
    list_buffer_group_timestamp_last,
    count_buffer,
    count_buffer_by_earlier,
    count_buffer_by_later,
    count_buffer_by_range,
    count_buffer_group,
    count_buffer_group_by_earlier,
    count_buffer_group_by_later,
    count_buffer_group_by_range
} from './buffer.js';
export {
    read_slice,
    list_slice_by_ids,
    list_slice_by_time,
    list_slice_by_range,
    list_slice_by_name_time,
    list_slice_by_name_range,
    list_slice_option,
    list_slice_group_by_time,
    list_slice_group_by_range,
    list_slice_group_option,
    create_slice,
    update_slice,
    delete_slice,
    read_slice_set,
    list_slice_set_by_ids,
    list_slice_set_by_time,
    list_slice_set_by_range,
    list_slice_set_by_name_time,
    list_slice_set_by_name_range,
    list_slice_set_option,
    create_slice_set,
    update_slice_set,
    delete_slice_set
} from './slice.js';
