from ..proto.auth import role_pb2, role_pb2_grpc
from typing import Optional, List
from uuid import UUID
import grpc
from ._schema import RoleSchema


def read_role(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleId(id=id.bytes)
        response = stub.ReadRole(request=request, metadata=auth.metadata)
        return RoleSchema.from_response(response.result)

def read_role_by_name(auth, api_id: UUID, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleName(api_id=api_id.bytes, name=name)
        response = stub.ReadRoleByName(request=request, metadata=auth.metadata)
        return RoleSchema.from_response(response.result)

def list_role_by_ids(auth, ids: List[UUID]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListRoleByIds(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleSchema.from_response(result))
        return ls

def list_role_by_api(auth, api_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.ApiId(api_id=api_id.bytes)
        response = stub.ListRoleByApi(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleSchema.from_response(result))
        return ls

def list_role_by_user(auth, user_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.UserId(id=user_id.bytes)
        response = stub.ListRoleByUser(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleSchema.from_response(result))
        return ls

def list_role_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleName(name=name)
        response = stub.ListRoleByName(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleSchema.from_response(result))
        return ls

def list_role_option(auth, api_id: Optional[UUID], user_id: Optional[UUID], name: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        api_bytes = None
        if api_id != None: api_bytes = api_id.bytes
        user_bytes = None
        if user_id != None: user_bytes = user_id.bytes
        request = role_pb2.RoleOption(
            api_id=api_bytes,
            user_id=user_bytes,
            name=name
        )
        response = stub.ListRoleOption(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleSchema.from_response(result))
        return ls

def create_role(auth, id: UUID, api_id: UUID, name: str, multi: bool, ip_lock: bool, access_duration: int, refresh_duration: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleSchema(
            id=id.bytes,
            api_id=api_id.bytes,
            name=name,
            multi=multi,
            ip_lock=ip_lock,
            access_duration=access_duration,
            refresh_duration=refresh_duration
        )
        response = stub.CreateRole(request=request, metadata=auth.metadata)
        return UUID(bytes=response.id)

def update_role(auth, id: UUID, name: Optional[str], multi: Optional[bool], ip_lock: Optional[bool], access_duration: Optional[int], refresh_duration: Optional[int]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleUpdate(
            id=id.bytes,
            name=name,
            multi=multi,
            ip_lock=ip_lock,
            access_duration=access_duration,
            refresh_duration=refresh_duration
        )
        stub.UpdateRole(request=request, metadata=auth.metadata)

def delete_role(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleId(id=id.bytes)
        stub.DeleteRole(request=request, metadata=auth.metadata)

def add_role_access(auth, id: UUID, procedure_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleAccess(id=id.bytes, procedure_id=procedure_id.bytes)
        stub.AddRoleAccess(request=request, metadata=auth.metadata)

def remove_role_access(auth, id: UUID, procedure_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = role_pb2_grpc.RoleServiceStub(channel)
        request = role_pb2.RoleAccess(id=id.bytes, procedure_id=procedure_id.bytes)
        stub.RemoveRoleAccess(request=request, metadata=auth.metadata)

