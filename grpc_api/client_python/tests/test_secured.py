import os
import sys

SOURCE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),"src")
sys.path.append(SOURCE_PATH)

import uuid
import dotenv
import pytest
from bbthings_grpc import Auth, Resource, DataType
from bbthings_grpc.common.utility import generate_access_key
from bbthings_grpc.common import ROOT_ID, ROOT_NAME
import utility


API_ID = uuid.uuid4()
API_PASSWORD = "Api_pa55w0rd"

PROCEDURE_ACCESS = [
    {
        "procedure": "read_model",
        "roles": ["admin", "user"]
    },
    {
        "procedure": "create_model",
        "roles": ["admin"]
    },
    {
        "procedure": "update_model",
        "roles": ["admin"]
    },
    {
        "procedure": "delete_model",
        "roles": ["admin"]
    }
]
PROCEDURES = [(uuid.uuid4(), access["procedure"]) for access in PROCEDURE_ACCESS]
ROLE_NAMES = set()
for item in PROCEDURE_ACCESS:
    for role in item["roles"]: ROLE_NAMES.add(role)
ROLES = [(uuid.uuid4(), role) for role in ROLE_NAMES]
role_access = []

ROOT_PASSWORD = "r0ot_P4s5w0rd"
ADMIN_ID = uuid.uuid4()
ADMIN_NAME = "administrator"
ADMIN_PASSWORD = "Adm1n_P4s5w0rd"
USER_ID = uuid.uuid4()
USER_NAME = "username"
USER_PASSWORD = "Us3r_P4s5w0rd"
USERS = [
    {
        "id": ADMIN_ID,
        "name": ADMIN_NAME,
        "password": ADMIN_PASSWORD,
        "role": "admin"
    },
    {
        "id": USER_ID,
        "name": USER_NAME,
        "password": USER_PASSWORD,
        "role": "user"
    }
]
user_role = []


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
    auth_root = Auth(address_auth)
    auth_root.login(ROOT_NAME, ROOT_PASSWORD)
    assert auth_root.user_id == ROOT_ID

    # create api and procedures
    api_id = auth_root.create_api(
        id=API_ID,
        name="resource api",
        address="localhost",
        category="RESOURCE",
        description="",
        password=API_PASSWORD,
        access_key=generate_access_key()
    )
    assert api_id == API_ID
    for (proc_id, proc_name) in PROCEDURES:
        id_proc = auth_root.create_procedure(
            id=proc_id,
            api_id=api_id,
            name=proc_name,
            description=""
        )
        assert id_proc == proc_id

    # create roles and link it to procedures
    for (role_id, role_name) in ROLES:
        id_role = auth_root.create_role(
            id=role_id,
            api_id=API_ID,
            name=role_name,
            multi=False,
            ip_lock=True,
            access_duration=900,
            refresh_duration=43200
        )
        assert id_role == role_id
        for access in PROCEDURE_ACCESS:
            if any(role == role_name for role in access["roles"]):
                found = next((p for p in PROCEDURES if p[1] == access["procedure"]), None)
                procedure_id = found[0] if found else None
                add = auth_root.add_role_access(role_id, procedure_id)
                assert add == None
                role_access.append((role_id, procedure_id))
                

    # create users and link it to a role
    for user in USERS:
        user_id = auth_root.create_user(
            id=user["id"],
            name=user["name"],
            email="",
            phone="",
            password=user["password"]
        )
        assert user_id == user["id"]
        found = next((r for r in ROLES if r[1] == user["role"]), None)
        role_id = found[0] if found else None
        add = auth_root.add_user_role(user["id"], role_id)
        assert add == None
        user_role.append((user["id"], role_id))

    # test newly created admin user and regular user to login to Auth server
    auth_admin = Auth(address_auth)
    auth_admin.login(ADMIN_NAME, ADMIN_PASSWORD)
    assert auth_admin.user_id == ADMIN_ID
    auth_admin.logout()
    assert auth_admin.user_id == None
    auth_user = Auth(address_auth)
    auth_user.login(USER_NAME, USER_PASSWORD)
    assert auth_user.user_id == USER_ID
    auth_user.logout()
    assert auth_user.user_id == None

    # start resource server for testing
    utility.start_resource_server(secured=True, api_id=api_id.hex, password=API_PASSWORD)

    # admin and regular user login
    res_admin = Resource(address_resource)
    res_admin.login(address_auth, ADMIN_NAME, ADMIN_PASSWORD)
    assert res_admin.api_id() == API_ID
    assert res_admin.user_id == ADMIN_ID
    res_user = Resource(address_resource)
    res_user.login(address_auth, USER_NAME, USER_PASSWORD)
    assert res_user.api_id() == API_ID
    assert res_user.user_id == USER_ID

    # try to create model by regular user and admin user, regular user should failed and admin user should success
    with pytest.raises(Exception):
        res_user.create_model(uuid.uuid4(), "UPLINK", "name", "")
    model_id = res_admin.create_model(
        id=uuid.uuid4(),
        data_type=[DataType.F64],
        category="UPLINK",
        name="name",
        description=""
    )

    # read created model by regular user
    model = res_user.read_model(model_id)
    assert model.category == "UPLINK"
    assert model.name == "name"

    # refresh regular user
    old_token = res_user.refresh_token
    res_user.refresh()
    assert res_user.refresh_token != old_token
    # try to read model again after refreshing token
    res_user.read_model(model_id)

    # regular user and admin user logout
    res_user.logout()
    assert res_user.auth_token == None
    res_admin.logout()
    assert res_admin.auth_token == None

    # try to access resource after logout, regular and admin user should failed
    with pytest.raises(Exception):
        res_user.read_model(model_id)
    with pytest.raises(Exception):
        res_admin.read_model(model_id)

    # remove user links to role and delete user
    for (user_id, role_id) in user_role:
        auth_root.remove_user_role(user_id, role_id)
        auth_root.delete_user(user_id)

    # remove role links to procedure and delete roles
    for (role_id, proc_id) in role_access:
        auth_root.remove_role_access(role_id, proc_id)
    for (role_id, _) in ROLES:
        auth_root.delete_role(role_id)

    # delete procedures and api
    for (proc_id, _) in PROCEDURES:
        auth_root.delete_procedure(proc_id)
    auth_root.delete_api(API_ID)

    # root logout
    auth_root.logout()
    assert auth_root.auth_token == None

    # try to read api after logout, should error
    with pytest.raises(Exception):
        auth_root.read_api(API_ID)

    # stop auth and resource server
    utility.stop_auth_server()
    utility.stop_resource_server()
