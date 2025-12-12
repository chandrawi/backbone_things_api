import pb_auth from "../proto/auth/auth_grpc_web_pb.js";
import {
    metadata,
    base64_to_uuid_hex,
    uuid_hex_to_base64,
    string_to_array_buffer,
    importKey,
    encryptMessage
} from "../common/utility.js";


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * @typedef {Object} ServerConfig
 * @property {string} address
 * @property {?string} auth_token
 */

/**
 * @typedef {Object} UserKeyResponse
 * @property {string} public_key
 */

/**
 * @typedef {Object} UserLoginRequest
 * @property {string} username
 * @property {string} password
 */

/**
 * @typedef {Object} AccessTokenMap
 * @property {Uuid} api_id
 * @property {string} access_token
 * @property {string} refresh_token
 */

/**
 * @param {*} r 
 * @returns {AccessTokenMap}
 */
function get_access_token(r) {
    return {
        api_id: base64_to_uuid_hex(r.apiId),
        access_token: r.accessToken,
        refresh_token: r.refreshToken
    };
}

/**
 * @typedef {Object} UserLoginResponse
 * @property {Uuid} user_id
 * @property {string} auth_token
 * @property {AccessTokenMap[]} access_tokens
 */

/**
 * @param {*} r 
 * @returns {UserLoginResponse}
 */
function get_login_response(r) {
    return {
        user_id: base64_to_uuid_hex(r.userId),
        auth_token: r.authToken,
        access_tokens: r.accessTokensList.map((v) => {return get_access_token(v)})
    };
}

/**
 * @typedef {Object} UserRefreshRequest
 * @property {Uuid} api_id
 * @property {string} access_token
 * @property {string} refresh_token
 */

/**
 * @typedef {Object} UserRefreshResponse
 * @property {string} access_token
 * @property {string} refresh_token
 */

/**
 * @param {*} r 
 * @returns {UserRefreshResponse}
 */
function get_refresh_response(r) {
    return {
        access_token: r.accessToken,
        refresh_token: r.refreshToken
    };
}

/**
 * @typedef {Object} UserLogoutRequest
 * @property {Uuid} user_id
 * @property {string} auth_token
 */


/**
 * Get user login key
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {} request empty object
 * @return {Promise<UserKeyResponse>} user key: public_key
 */
export async function user_login_key(config, request) {
    const client = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userKeyRequest = new pb_auth.UserKeyRequest();
    return client.userPasswordKey(userKeyRequest)
        .then(response => response.toObject());
}

/**
 * User login
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserLoginRequest} request user login request: username, password
 * @return {Promise<UserLoginResponse>} user login response: user_id, auth_token, access_tokens
 */
export async function user_login(config, request) {
    const client = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userKeyRequest = new pb_auth.UserKeyRequest();
    const userLoginRequest = new pb_auth.UserLoginRequest();
    userLoginRequest.setUsername(request.username);
    const key = await client.userPasswordKey(userKeyRequest)
        .then(response => response.toObject().publicKey);
    const pubkey = await importKey(key);
    const ciphertext = await encryptMessage(request.password, pubkey);
    userLoginRequest.setPassword(ciphertext);
    return client.userLogin(userLoginRequest)
        .then(response => get_login_response(response.toObject()));
}

/**
 * Refresh user token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserRefreshRequest} request user refresh request: api_id, access_token, refresh_token
 * @return {Promise<UserRefreshResponse>} user refresh response: access_token, refresh_token
 */
export async function user_refresh(config, request) {
    const client = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userRefreshRequest = new pb_auth.UserRefreshRequest();
    userRefreshRequest.setApiId(uuid_hex_to_base64(request.api_id));
    userRefreshRequest.setAccessToken(request.access_token);
    userRefreshRequest.setRefreshToken(request.refresh_token);
    return client.userRefresh(userRefreshRequest)
        .then(response => get_refresh_response(response.toObject()));
}

/**
 * User logout
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserLogoutRequest} request user logout request: user_id, auth_token
 * @return {Promise<null>} user logout response
 */
export async function user_logout(config, request) {
    const client = new pb_auth.AuthServicePromiseClient(config.address, null, null);
    const userLogoutRequest = new pb_auth.UserLogoutRequest();
    userLogoutRequest.setUserId(uuid_hex_to_base64(request.user_id));
    userLogoutRequest.setAuthToken(request.auth_token);
    return client.userLogout(userLogoutRequest)
        .then(response => null);
}
