import pb_auth from "../proto/auth/auth_grpc_web_pb.js";
import {
    base64_to_uuid_hex,
    uuid_hex_to_base64,
    importKey,
    encryptMessage
} from "../common/utility.js";


/**
 * @typedef {(string|Uint8Array)} Uuid
 */

/**
 * Authorization/Authentication server configuration.
 * @param {String} address - Resource server address
 * @param {?Uuid} user_id - ID of the user connected to the server
 * @param {?String} auth_token - User identification token. Used for logout
 */
export class AuthConfig {

    constructor(address, auth_token) {
        this.address = address;
        this.auth_token = auth_token;
    }

    /**
     * Login user to Authorization/Authentication server for accesing resource
     * @param {String} username
     * @param {String} password
     */
    async login(username, password) {
        // login user to auth server
        const client = new pb_auth.AuthServicePromiseClient(this.address, null, null);
        const userKeyRequest = new pb_auth.UserKeyRequest();
        const key = await client.userPasswordKey(userKeyRequest)
            .then(response => response.toObject().publicKey);
        const userLoginRequest = new pb_auth.UserLoginRequest();
        userLoginRequest.setUsername(username);
        const pubkey = await importKey(key);
        const ciphertext = await encryptMessage(password, pubkey);
        userLoginRequest.setPassword(ciphertext);
        const login = await client.userLogin(userLoginRequest)
            .then(response => response.toObject());
        this.auth_token = login.authToken;
        this.user_id = base64_to_uuid_hex(login.userId);
    }

    /**
     * Logout user from Authorization/Authentication server
     */
    async logout() {
        if (this.address && this.user_id) {
            const client = new pb_auth.AuthServicePromiseClient(this.address, null, null);
            const userLogoutRequest = new pb_auth.UserLogoutRequest();
            userLogoutRequest.setUserId(uuid_hex_to_base64(this.user_id));
            userLogoutRequest.setAuthToken(this.auth_token);
            await client.userLogout(userLogoutRequest)
                .then(response => response.toObject());
            this.user_id = undefined;
            this.auth_token = undefined;
        }
    }

}


export {
    read_api,
    read_api_by_name,
    list_api_by_ids,
    list_api_by_name,
    list_api_by_category,
    list_api_option,
    create_api,
    update_api,
    delete_api,
    read_procedure,
    read_procedure_by_name,
    list_procedure_by_ids,
    list_procedure_by_api,
    list_procedure_by_name,
    list_procedure_option,
    create_procedure,
    update_procedure,
    delete_procedure
} from './api.js';
export {
    read_role,
    read_role_by_name,
    list_role_by_ids,
    list_role_by_api,
    list_role_by_user,
    list_role_by_name,
    list_role_option,
    create_role,
    update_role,
    delete_role,
    add_role_access,
    remove_role_access
} from './role.js';
export {
    read_user,
    read_user_by_name,
    list_user_by_ids,
    list_user_by_api,
    list_user_by_role,
    list_user_by_name,
    list_user_option,
    create_user,
    update_user,
    delete_user,
    add_user_role,
    remove_user_role
} from './user.js';
export {
    read_role_profile,
    list_role_profile_by_role,
    create_role_profile,
    update_role_profile,
    delete_role_profile,
    read_user_profile,
    list_user_profile_by_user,
    create_user_profile,
    update_user_profile,
    delete_user_profile,
    swap_user_profile
} from './profile.js';
export {
    read_access_token,
    list_auth_token,
    list_token_by_user,
    list_token_by_created_earlier,
    list_token_by_created_later,
    list_token_by_created_range,
    list_token_by_expired_earlier,
    list_token_by_expired_later,
    list_token_by_expired_range,
    list_token_by_range,
    create_access_token,
    create_auth_token,
    update_access_token,
    update_auth_token,
    delete_access_token,
    delete_auth_token,
    delete_token_by_user
} from './token.js';
export {
    user_login_key,
    user_login,
    user_refresh,
    user_logout
} from './auth.js';
