from ..proto.auth import user_pb2, user_pb2_grpc
from typing import Optional, List
from uuid import UUID
import grpc
from ._schema import UserSchema


def read_user(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserId(id=id.bytes)
        response = stub.ReadUser(request=request, metadata=auth.metadata)
        return UserSchema.from_response(response.result)

def read_user_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserName(name=name)
        response = stub.ReadUserByName(request=request, metadata=auth.metadata)
        return UserSchema.from_response(response.result)

def list_user_by_ids(auth, ids: List[UUID]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListUserByIds(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserSchema.from_response(result))
        return ls

def list_user_by_api(auth, api_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.ApiId(id=api_id.bytes)
        response = stub.ListUserByApi(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserSchema.from_response(result))
        return ls

def list_user_by_role(auth, role_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.RoleId(id=role_id.bytes)
        response = stub.ListUserByRole(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserSchema.from_response(result))
        return ls

def list_user_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserName(name=name)
        response = stub.ListUserByName(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserSchema.from_response(result))
        return ls

def list_user_option(auth, api_id: Optional[UUID], role_id: Optional[UUID], name: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        api_bytes = None
        if api_id != None: api_bytes = api_id.bytes
        role_bytes = None
        if role_id != None: role_bytes = role_id.bytes
        request = user_pb2.UserOption(
            api_id=api_bytes,
            role_id=role_bytes,
            name=name
        )
        response = stub.ListUserOption(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserSchema.from_response(result))
        return ls

def create_user(auth, id: UUID, name: str, email: str, phone: str, password: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserSchema(
            id=id.bytes,
            name=name,
            email=email,
            phone=phone,
            password=password
        )
        response = stub.CreateUser(request=request, metadata=auth.metadata)
        return UUID(bytes=response.id)

def update_user(auth, id: UUID, name: Optional[str], email: Optional[str], phone: Optional[str], password: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserUpdate(
            id=id.bytes,
            name=name,
            email=email,
            phone=phone,
            password=password
        )
        stub.UpdateUser(request=request, metadata=auth.metadata)

def delete_user(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserId(id=id.bytes)
        stub.DeleteUser(request=request, metadata=auth.metadata)

def add_user_role(auth, id: UUID, role_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserRole(user_id=id.bytes, role_id=role_id.bytes)
        stub.AddUserRole(request=request, metadata=auth.metadata)

def remove_user_role(auth, id: UUID, role_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)
        request = user_pb2.UserRole(user_id=id.bytes, role_id=role_id.bytes)
        stub.RemoveUserRole(request=request, metadata=auth.metadata)
