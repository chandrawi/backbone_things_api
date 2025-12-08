import os
import sys

SOURCE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),"src")
sys.path.append(SOURCE_PATH)

import uuid
import dotenv
import pytest
import string
from bbthings_grpc import Auth, Resource, DataType
from bbthings_grpc.common.utility import hash_password, generate_access_key
import utility

ACCESSES = [
    ("read_model", ["admin", "user"]),
    ("create_model", ["admin"]),
    ("delete_model", ["admin"]),
    ("change_model_type", ["admin"])
]

ROLES = ["admin", "user"]

ROOT_NAME = "root"
ADMIN_NAME = "administrator"
USER_NAME = "username"
ROOT_PW = "r0ot_P4s5w0rd"
ADMIN_PW = "Adm1n_P4s5w0rd"
USER_PW = "Us3r_P4s5w0rd"

USERS = [
    (ADMIN_NAME, ADMIN_PW, "admin"),
    (USER_NAME, USER_PW, "user")
]

def test_secured():
    dotenv.load_dotenv()
    address_auth = os.getenv('SERVER_ADDRESS_AUTH')
    address_resource = os.getenv('SERVER_ADDRESS_RESOURCE')
    db_auth_url_test = os.getenv("DATABASE_URL_AUTH_TEST")
    db_resource_url_test = os.getenv("DATABASE_URL_RESOURCE_TEST")

    # truncate auth and resource tables before testing
    utility.truncate_tables_auth(db_auth_url_test)
    utility.truncate_tables_resource(db_resource_url_test)

    # start auth server for testing
    utility.start_auth_server(secured=True)

    # root login
    auth = Auth(address_auth)
    login = auth.user_login(ROOT_NAME, ROOT_PW)
    root_id = login.user_id
    root_auth_token = login.auth_token
    root_access = login.access_tokens[0]

    # create root auth instance
    auth_root = Auth(address_auth, root_auth_token)

    # create api and procedures
    api_password = "Api_pa55w0rd"
    api_id = auth_root.create_api(
        id=uuid.uuid4(),
        name="resource api",
        address="localhost",
        category="RESOURCE",
        description="",
        password=hash_password(api_password),
        access_key=generate_access_key()
    )
    proc_map = []
    for (procedure_name, _) in ACCESSES:
        proc_id = auth_root.create_procedure(
            id=uuid.uuid4(),
            api_id=api_id,
            name=procedure_name,
            description=""
        )
        proc_map.append((proc_id, procedure_name))

    # create roles and link it to procedures
    role_map = []
    for name in ROLES:
        role_id = auth_root.create_role(
            id=uuid.uuid4(),
            api_id=api_id,
            name=name,
            multi=False,
            ip_lock=True,
            access_duration=900,
            refresh_duration=43200
        )
        role_map.append((role_id, name))
    role_accesses = []
    for (procedure_name, roles) in ACCESSES:
        proc_filter = list(filter(lambda x: x[1] == procedure_name, proc_map))
        if len(proc_filter) == 0: continue
        proc_id = proc_filter[0][0]
        for role in roles:
            role_filter = list(filter(lambda x: x[1] == role, role_map))
            if len(role_filter) == 0: continue
            role_id = role_filter[0][0]
            role_accesses.append((role_id, proc_id))
    for (role_id, proc_id) in role_accesses:
        auth_root.add_role_access(role_id, proc_id)

    # create users and link it to a role
    user_roles = []
    for (name, password, role) in USERS:
        user_id = auth_root.create_user(
            id=uuid.uuid4(),
            name=name,
            email="",
            phone="",
            password=hash_password(password)
        )
        role_filter = list(filter(lambda x: x[1] == role, role_map))
        if len(role_filter) == 0: continue
        role_id = role_filter[0][0]
        user_roles.append((user_id, role_id))
    for (user_id, role_id) in user_roles:
        auth_root.add_user_role(user_id, role_id)

    # start resource server for testing
    utility.start_resource_server(secured=True, api_id=api_id.hex, password=api_password)

    # user and admin login
    login = auth.user_login(ADMIN_NAME, ADMIN_PW)
    admin_id = login.user_id
    admin_auth_token = login.auth_token
    admin_access = login.access_tokens[0]
    login = auth.user_login(USER_NAME, USER_PW)
    user_id = login.user_id
    user_auth_token = login.auth_token
    user_access = login.access_tokens[0]

    # create admin and regular user resource instance
    res_admin = Resource(address_resource, admin_access.access_token)
    res_user = Resource(address_resource, user_access.access_token)

    # try to create model using user service and admin service, user should failed and admin should success
    with pytest.raises(Exception):
        res_user.create_model(uuid.uuid4(), "UPLINK", "name", "")
    model_id = res_admin.create_model(
        id=uuid.uuid4(),
        data_type=[DataType.F64],
        category="UPLINK",
        name="name",
        description=""
    )

    # read created model using user service
    model = res_user.read_model(model_id)
    assert model.category == "UPLINK"
    assert model.name == "name"

    # refresh user
    user_refresh = auth.user_refresh(api_id, user_access.access_token, user_access.refresh_token)
    res_user = Resource(address_resource, user_refresh.access_token)
    # try to read model again after refreshing token
    res_user.read_model(model_id)

    # remove model type and delete model
    res_admin.delete_model(model_id)
    with pytest.raises(Exception):
        res_admin.read_model(model_id)

    # user and admin logout
    auth.user_logout(admin_id, admin_auth_token)
    auth.user_logout(user_id, user_auth_token)

    # remove user links to role and delete user
    for (user_id, role_id) in user_roles:
        auth_root.remove_user_role(user_id, role_id)
        auth_root.delete_user(user_id)

    # remove role links to procedure and delete roles
    for (role_id, proc_id) in role_accesses:
        auth_root.remove_role_access(role_id, proc_id)
    for (role_id, _) in role_map:
        auth_root.delete_role(role_id)

    # delete procedures and api
    for (proc_id, _) in proc_map:
        auth_root.delete_procedure(proc_id)
    auth_root.delete_api(api_id)

    # root logout
    auth_root.user_logout(root_id, root_auth_token)

    # try to read api after logout, should error
    with pytest.raises(Exception):
        auth_root.read_api(api_id)

    # stop auth and resource server
    utility.stop_auth_server()
    utility.stop_resource_server()
