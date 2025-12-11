import pb_token from "../proto/auth/token_grpc_web_pb.js";
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
 * @property {?string} auth_token
 */

/**
 * @typedef {Object} AccessId
 * @property {number} access_id
 */

/**
 * @typedef {Object} AuthToken
 * @property {string} auth_token
 */

/**
 * @typedef {Object} UserId
 * @property {Uuid} id
 */

/**
 * @typedef {Object} TokenSchema
 * @property {number} access_id
 * @property {Uuid} user_id
 * @property {string} refresh_token
 * @property {string} auth_token
 * @property {Date} created
 * @property {Date} expired
 * @property {number[]|Uint8Array} ip
 */

/**
 * @param {*} r 
 * @returns {TokenSchema}
 */
function get_token_schema(r) {
    return {
        access_id: r.accessId,
        user_id: base64_to_uuid_hex(r.userId),
        refresh_token: r.refreshToken,
        auth_token: r.authToken,
        expired: new Date(r.expired / 1000),
        ip: base64_to_bytes(r.ip)
    };
}

/**
 * @param {*} r 
 * @returns {TokenSchema[]}
 */
function get_token_schema_vec(r) {
    return r.map((v) => {return get_token_schema(v)});
}

/**
 * @typedef {Object} TokenEarlier
 * @property {Date} earlier
 * @property {?Uuid} user_id
 */

/**
 * @typedef {Object} TokenLater
 * @property {Date} later
 * @property {?Uuid} user_id
 */

/**
 * @typedef {Object} TokenRange
 * @property {Date} begin
 * @property {Date} end
 * @property {?Uuid} user_id
 */

/**
 * @typedef {Object} TokenRanges
 * @property {Date} created_begin
 * @property {Date} created_end
 * @property {Date} expired_begin
 * @property {Date} expired_end
 * @property {?Uuid} user_id
 */

/**
 * @typedef {Object} AuthTokenCreate
 * @property {Uuid} user_id
 * @property {Date} expired
 * @property {number[]|Uint8Array} ip
 * @property {number} number
 */

/**
 * @typedef {Object} TokenUpdate
 * @property {?number} access_id
 * @property {?string} refresh_token
 * @property {?string} auth_token
 * @property {?Date} expired
 * @property {?number[]|Uint8Array} ip
 */

/**
 * @typedef {Object} TokenCreateResponse
 * @property {number} access_id
 * @property {string} refresh_token
 * @property {string} auth_token
 */

/**
 * @param {*} r 
 * @returns {TokenCreateResponse}
 */
function get_token_create_response(r) {
    return {
        access_id: r.accessId,
        refresh_token: r.refreshToken,
        auth_token: r.authToken,
    };
}

/**
 * @param {*} r 
 * @returns {TokenCreateResponse[]}
 */
function get_token_create_response_vec(r) {
    return r.map((v) => {return get_token_create_response(v)});
}

/**
 * @typedef {Object} TokenUpdateResponse
 * @property {string} refresh_token
 * @property {string} auth_token
 */

/**
 * @param {*} r 
 * @returns {TokenUpdateResponse}
 */
function get_token_update_response(r) {
    return {
        refresh_token: r.refreshToken,
        auth_token: r.authToken,
    };
}


/**
 * Read an access token by access id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {AccessId} request access id: access_id
 * @returns {Promise<TokenSchema>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function read_access_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const accessId = new pb_token.AccessId();
    accessId.setAccessId(request.access_id);
    return client.readAccessToken(accessId, metadata(config.auth_token))
        .then(response => get_token_schema(response.toObject().result));
}

/**
 * Read tokens by auth token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {AuthToken} request auth token: auth_token
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_auth_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const authToken = new pb_token.AuthToken();
    authToken.setAuthToken(request.auth_token);
    return client.listAuthToken(authToken, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by user id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserId} request user id: id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_user(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const userId = new pb_token.UserId();
    userId.setUserId(uuid_hex_to_base64(request.id));
    return client.listTokenByUser(userId, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by earlier created time
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenEarlier} request token earlier: earlier, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_created_earlier(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenTime = new pb_token.TokenTime();
    tokenTime.setTimestamp(request.earlier.valueOf() * 1000);
    if (request.user_id) {
        tokenTime.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByCreatedEarlier(tokenTime, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by later created time
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenLater} request token later: later, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_created_later(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenTime = new pb_token.TokenTime();
    tokenTime.setTimestamp(request.later.valueOf() * 1000);
    if (request.user_id) {
        tokenTime.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByCreatedLater(tokenTime, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by created time range
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenRange} request token range: begin, end, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_created_range(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenRange = new pb_token.TokenRangeSingle();
    tokenRange.setBegin(request.begin.valueOf() * 1000);
    tokenRange.setEnd(request.end.valueOf() * 1000);
    if (request.user_id) {
        tokenRange.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByCreatedRange(tokenRange, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by earlier expired time
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenEarlier} request token earlier: earlier, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_expired_earlier(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenTime = new pb_token.TokenTime();
    tokenTime.setTimestamp(request.earlier.valueOf() * 1000);
    if (request.user_id) {
        tokenTime.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByExpiredEarlier(tokenTime, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by later expired time
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenLater} request token later: later, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_expired_later(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenTime = new pb_token.TokenTime();
    tokenTime.setTimestamp(request.later.valueOf() * 1000);
    if (request.user_id) {
        tokenTime.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByExpiredLater(tokenTime, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by expired time range
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenRange} request token range: begin, end, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_expired_range(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenRange = new pb_token.TokenRangeSingle();
    tokenRange.setBegin(request.begin.valueOf() * 1000);
    tokenRange.setEnd(request.end.valueOf() * 1000);
    if (request.user_id) {
        tokenRange.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByExpiredRange(tokenRange, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Read tokens by created and expired time range
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenRanges} request token ranges: begin, end, user_id
 * @returns {Promise<TokenSchema[]>} token schema: access_id, user_id, refresh_token, auth_token, created, expired, ip
 */
export async function list_token_by_range(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenRanges = new pb_token.TokenRangeDouble();
    tokenRanges.setBegin1(request.created_begin.valueOf() * 1000);
    tokenRanges.setEnd1(request.created_end.valueOf() * 1000);
    tokenRanges.setBegin2(request.expired_begin.valueOf() * 1000);
    tokenRanges.setEnd2(request.expired_end.valueOf() * 1000);
    if (request.user_id) {
        tokenRanges.setUserId(uuid_hex_to_base64(request.user_id));
    }
    return client.listTokenByRange(tokenRanges, metadata(config.auth_token))
        .then(response => get_token_schema_vec(response.toObject().resultsList));
}

/**
 * Create an access token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenSchema} request token schema: user_id, auth_token, expired, expired, ip
 * @returns {Promise<TokenCreateResponse>} create response: access_id, refresh_token, auth_token
 */
export async function create_access_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenSchema = new pb_token.TokenSchema();
    tokenSchema.setUserId(uuid_hex_to_base64(request.user_id));
    tokenSchema.setAuthToken(request.auth_token);
    tokenSchema.setExpired(request.expired.valueOf() * 1000);
    tokenSchema.setIp(bytes_to_base64(request.ip));
    return client.createAccessToken(tokenSchema, metadata(config.auth_token))
        .then(response => get_token_create_response(response.toObject()));
}

/**
 * Create tokens with shared auth token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {AuthTokenCreate} request token schema: user_id, expired, ip, number
 * @returns {Promise<TokenCreateResponse[]>} create response: access_id, refresh_token, auth_token
 */
export async function create_auth_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const authTokenCreate = new pb_token.AuthTokenCreate();
    authTokenCreate.setUserId(uuid_hex_to_base64(request.user_id));
    authTokenCreate.setExpired(request.expired.valueOf() * 1000);
    authTokenCreate.setIp(bytes_to_base64(request.ip));
    authTokenCreate.setNumber(request.number);
    return client.createAuthToken(authTokenCreate, metadata(config.auth_token))
        .then(response => get_token_create_response_vec(response.toObject().tokensList));
}

/**
 * Update an access token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenUpdate} request token update: access_id, expired, ip
 * @returns {Promise<TokenUpdateResponse>} update response: refresh_token, auth_token
 */
export async function update_access_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenUpdate = new pb_token.TokenUpdate();
    tokenUpdate.setAccessId(request.access_id);
    if (request.expired instanceof Date) {
        tokenUpdate.setExpired(request.expired.valueOf() * 1000);
    }
    if (request.ip) {
        tokenUpdate.setIp(bytes_to_base64(request.ip));
    }
    return client.updateAccessToken(tokenUpdate, metadata(config.auth_token))
        .then(response => get_token_update_response(response.toObject()));
}

/**
 * Update all tokens with shared auth token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {TokenUpdate} request token update: auth_token, expired, ip
 * @returns {Promise<TokenUpdateResponse>} update response: refresh_token, auth_token
 */
export async function update_auth_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const tokenUpdate = new pb_token.TokenUpdate();
    tokenUpdate.setAuthToken(request.auth_token);
    if (request.expired instanceof Date) {
        tokenUpdate.setExpired(request.expired.valueOf() * 1000);
    }
    if (request.ip) {
        tokenUpdate.setIp(bytes_to_base64(request.ip));
    }
    return client.updateAuthToken(tokenUpdate, metadata(config.auth_token))
        .then(response => get_token_update_response(response.toObject()));
}

/**
 * Delete an access token by access id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {AccessId} request access id: access_id
 * @returns {Promise<{}>} delete response
 */
export async function delete_access_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const accessId = new pb_token.AccessId();
    accessId.setAccessId(request.access_id);
    return client.deleteAccessToken(accessId, metadata(config.auth_token))
        .then(response => response.toObject());
}

/**
 * Delete tokens by auth token
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {AuthToken} request auth token: auth_token
 * @returns {Promise<{}>} delete response
 */
export async function delete_auth_token(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const authToken = new pb_token.AuthToken();
    authToken.setAuthToken(request.auth_token);
    return client.deleteAuthToken(authToken, metadata(config.auth_token))
        .then(response => response.toObject());
}

/**
 * Delete tokens by user id
 * @param {ServerConfig} config Auth server config: address, auth_token
 * @param {UserId} request user id: id
 * @returns {Promise<{}>} delete response
 */
export async function delete_token_by_user(config, request) {
    const client = new pb_token.TokenServicePromiseClient(config.address, null, null);
    const userId = new pb_token.UserId();
    userId.setUserId(uuid_hex_to_base64(request.id));
    return client.deleteTokenByUser(userId, metadata(config.auth_token))
        .then(response => response.toObject());
}
