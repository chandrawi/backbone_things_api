import os
import sys

SOURCE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),"src")
sys.path.append(SOURCE_PATH)

from datetime import datetime
import uuid
import dotenv
import pytest
from bbthings_grpc import Auth, DataType
from bbthings_grpc.common.utility import verify_password, generate_access_key
import utility


def test_auth():
    dotenv.load_dotenv()
    address = os.getenv('SERVER_ADDRESS_AUTH')
    db_auth_url_test = os.getenv("DATABASE_URL_AUTH_TEST")
    auth = Auth(address)

    # truncate auth tables before testing
    utility.truncate_tables_auth(db_auth_url_test)

    # start auth server for testing
    utility.start_auth_server()

    # create new resource API
    password_api = "Ap1_P4s5w0rd"
    access_key = generate_access_key()
    api_id1 = auth.create_api(uuid.uuid4(), "Resource1", "localhost:9001", "RESOURCE", "", password_api, access_key)
    api_id2 = auth.create_api(uuid.uuid4(), "Resource_2", "localhost:9002", "RESOURCE", "", password_api, access_key)

    # create new procedure for newly created resource API
    proc_id1 = auth.create_procedure(uuid.uuid4(), api_id1, "ReadResourceData", "")
    proc_id2 = auth.create_procedure(uuid.uuid4(), api_id1, "CreateData", "")
    proc_id3 = auth.create_procedure(uuid.uuid4(), api_id1, "DeleteData", "")
    proc_id4 = auth.create_procedure(uuid.uuid4(), api_id2, "ReadConfig", "")

    # get newly created resource at the first of resource API list
    apis = auth.list_api_by_category("RESOURCE")
    api_ids = []
    for api in apis: api_ids.append(api.id)
    api = auth.read_api(api_id1)
    api_proc_ids = []
    for proc in api.procedures: api_proc_ids.append(proc.id)

    # get newly created procedure at the first of procedure list
    procedures = auth.list_procedure_by_api(api_id1)
    proc_ids = []
    for proc in procedures: proc_ids.append(proc.id)

    assert api.name == "Resource1"
    assert api.address == "localhost:9001"
    assert api_id1 in api_ids
    assert proc_id1 in proc_ids
    assert api_proc_ids == proc_ids

    assert verify_password(password_api, api.password)

    # create new role and add access to the procedure
    role_id1 = auth.create_role(uuid.uuid4(), api_id1, "administrator", False, False, 900, 28800)
    auth.add_role_access(role_id1, proc_id1)
    auth.add_role_access(role_id1, proc_id2)
    auth.add_role_access(role_id1, proc_id3)
    role_id2 = auth.create_role(uuid.uuid4(), api_id1, "user", True, False, 900, 604800)
    auth.add_role_access(role_id2, proc_id1)
    role_id3 = auth.create_role(uuid.uuid4(), api_id2, "user", True, False, 900, 604800)
    auth.add_role_access(role_id3, proc_id4)

    # get role data
    roles = auth.list_role_by_api(api_id1)
    role_ids = []
    for role in roles: role_ids.append(role.id)
    role = auth.read_role(role_id1)

    assert role_id1 in role_ids
    assert role.name == "administrator"
    assert role.multi == False
    assert role.ip_lock == False
    assert proc_id1 in role.procedure_ids
    assert proc_id2 in role.procedure_ids
    assert proc_id3 in role.procedure_ids

    assert len(role.access_key) == 32

    # update a resource API and its procedure and role
    api_name = "Resource_1"
    proc_name = "ReadData"
    role_name = "admin"
    access_key_new = generate_access_key()
    auth.update_api(api_id1, api_name, None, None, "New resource api", None, access_key_new)
    auth.update_procedure(proc_id1, proc_name, "Read resource data")
    auth.update_role(role_id1, role_name, None, True, None, None)

    # get updated resource API schema
    api = auth.read_api_by_name(api_name)
    procedure = auth.read_procedure_by_name(api_id1, proc_name)
    role = auth.read_role_by_name(api_id1, role_name)

    assert api.name == api_name
    assert api.description == "New resource api"
    assert procedure.name == proc_name
    assert procedure.description == "Read resource data"
    assert role.name == role_name
    assert role.ip_lock == True
    assert role.access_key != access_key

    # create new user and add associated roles
    password_admin = "Adm1n_P4s5w0rd"
    user_id1 = auth.create_user(uuid.uuid4(), "administrator", "admin@mail.co", "+6281234567890", password_admin)
    auth.add_user_role(user_id1, role_id1)
    auth.add_user_role(user_id1, role_id3)
    password_user = "Us3r_P4s5w0rd"
    user_id2 = auth.create_user(uuid.uuid4(), "username", "user@mail.co", "+6281234567890", password_user)
    auth.add_user_role(user_id2, role_id2)
    auth.add_user_role(user_id2, role_id3)

    # get user data
    users = auth.list_user_by_role(role_id3)
    user_ids = []
    for user in users: user_ids.append(user.id)
    user = auth.read_user(user_id1)

    assert user_id1 in user_ids
    assert user.name == "administrator"
    assert user.email == "admin@mail.co"
    assert user.phone == "+6281234567890"

    assert verify_password(password_admin, user.password)

    # update user
    password_new = "N3w_P4s5w0rd"
    auth.update_user(user_id2, None, None, None, password_new)

    # get updated user
    old_password = user.password
    user = auth.read_user_by_name("username")

    assert user.password != old_password

    # create role and user profile
    profile_role_id1 = auth.create_role_profile(role_id1, "name", DataType.STRING, "", "")
    profile_role_id2 = auth.create_role_profile(role_id1, "age", DataType.U16, 0, "")
    profile_user_id1 = auth.create_user_profile(user_id1, "name", "john doe", "")
    profile_user_id2 = auth.create_user_profile(user_id1, "age", 20, "")

    # read role and user profile
    profile_role1 = auth.read_role_profile(profile_role_id1)
    profile_role2 = auth.read_role_profile(profile_role_id2)
    profile_user1 = auth.read_user_profile(profile_user_id1)
    profile_users = auth.list_user_profile_by_user(user_id1)

    assert profile_role1.name == "name"
    assert profile_role2.category == ""
    assert profile_user1.value == "john doe"
    assert profile_user1 in profile_users

    # update user profile
    auth.update_user_profile(profile_user_id2, None, 21, None)
    profile_user2 = auth.read_user_profile(profile_user_id2)

    assert profile_user2.value == 21

    # create new access token and refresh token
    expire1 = datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    expire2 = datetime.strptime("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
    auth_token = "rGKrHrDuWXt2CDbjmrt1SHbmea86wIQb"
    (access_id1, _, auth_token1) = auth.create_access_token(user_id1, auth_token, expire1, bytes([192, 168, 0, 1]))
    access_id2 = access_id1 + 1
    auth.create_auth_token(user_id1, expire2, bytes([192, 168, 0, 1]), 1)
    auth.create_access_token(user_id1, auth_token, expire1, bytes())

    # get token data
    access_token = auth.read_access_token(access_id2)
    auth_tokens = auth.list_auth_token(auth_token1)
    auth_token_filter = filter(lambda x: x.auth_token == auth_token1, auth_tokens)
    auth_token = list(auth_token_filter)[0]
    user_tokens = auth.list_token_by_user(user_id1)

    assert auth_token.user_id == user_id1
    assert auth_token.expired == expire1
    assert auth_token.ip == bytes([192, 168, 0, 1])
    assert access_token.expired == expire2
    assert len(user_tokens) == 3

    # update token
    expire3 = datetime.strptime("2023-01-01 18:00:00", "%Y-%m-%d %H:%M:%S")
    auth.update_access_token(access_id2, expire3, None)
    auth.update_auth_token(auth_token1, expire3, bytes([192, 168, 0, 100]))

    # get updated token
    new_access_token = auth.read_access_token(access_id2)
    new_auth_tokens = auth.list_auth_token(auth_token1)
    auth_token_filter = filter(lambda x: x.auth_token == auth_token1, new_auth_tokens)
    new_auth_token = list(auth_token_filter)[0]

    assert new_access_token.refresh_token != access_token.refresh_token
    assert new_access_token.expired == expire3
    assert new_auth_token.expired == expire3
    assert new_auth_token.ip == bytes([192, 168, 0, 100])

    # delete role and user profile
    auth.delete_user_profile(profile_user_id1)
    auth.delete_role_profile(profile_role_id1)

    # check if role and user profile already deleted
    with pytest.raises(Exception):
        auth.read_user_profile(profile_user_id1)
    with pytest.raises(Exception):
        auth.read_role_profile(profile_role_id1)

    # try to delete resource API, procedure role and user without removing dependent item
    with pytest.raises(Exception):
        auth.delete_role(role_id3)
    with pytest.raises(Exception):
        auth.delete_procedure(proc_id4)
    with pytest.raises(Exception):
        auth.delete_api(api_id2)
    with pytest.raises(Exception):
        auth.delete_user(user_id2)

    # delete user and token
    auth.remove_user_role(user_id2, role_id2)
    auth.remove_user_role(user_id2, role_id3)
    auth.delete_user(user_id2)
    auth.delete_token_by_user(user_id1)

    # check if token and user already deleted
    with pytest.raises(Exception):
        auth.read_access_token(access_id1)
    with pytest.raises(Exception):
        auth.read_user(user_id2)

    # delete resource API, procedure, and role
    auth.remove_user_role(user_id1, role_id3)
    auth.remove_role_access(role_id3, proc_id4)
    auth.delete_role(role_id3)
    auth.delete_procedure(proc_id4)
    auth.delete_api(api_id2)

    # check if resource API, procedure, and role already deleted
    with pytest.raises(Exception):
        auth.read_role(role_id3)
    with pytest.raises(Exception):
        auth.read_procedure(proc_id4)
    with pytest.raises(Exception):
        auth.read_api(api_id2)

    # stop auth server
    utility.stop_auth_server()
