from ..proto.auth import profile_pb2, profile_pb2_grpc
from typing import Optional, Union
from uuid import UUID
import grpc
from ._schema import RoleProfileSchema, UserProfileSchema, ProfileMode
from ..common.type_value import DataType, pack_data


def read_role_profile(auth, id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.ProfileId(id=id)
        response = stub.ReadRoleProfile(request=request, metadata=auth.metadata)
        return RoleProfileSchema.from_response(response.result)

def list_role_profile_by_role(auth, role_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.RoleId(id=role_id.bytes)
        response = stub.ListRoleProfile(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(RoleProfileSchema.from_response(result))
        return ls

def create_role_profile(auth, role_id: UUID, name: str, value_type: DataType, mode: Union[str, int]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.RoleProfileSchema(
            role_id=role_id.bytes,
            name=name,
            value_type=value_type.value,
            mode=ProfileMode.from_int_str(mode)
        )
        response = stub.CreateRoleProfile(request=request, metadata=auth.metadata)
        return response.id

def update_role_profile(auth, id: int, name: Optional[str], value_type: Optional[DataType], mode: Optional[Union[str, int]]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        ty = None
        if value_type != None: ty = value_type.value
        mode = None if mode == None else ProfileMode.from_int_str(mode)
        request = profile_pb2.RoleProfileUpdate(
            id=id,
            name=name,
            value_type=ty,
            mode=mode
        )
        stub.UpdateRoleProfile(request=request, metadata=auth.metadata)

def delete_role_profile(auth, id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.ProfileId(id=id)
        stub.DeleteRoleProfile(request=request, metadata=auth.metadata)

def read_user_profile(auth, id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.ProfileId(id=id)
        response = stub.ReadUserProfile(request=request, metadata=auth.metadata)
        return UserProfileSchema.from_response(response.result)

def list_user_profile_by_user(auth, user_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.UserId(id=user_id.bytes)
        response = stub.ListUserProfile(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(UserProfileSchema.from_response(result))
        return ls

def create_user_profile(auth, user_id: UUID, name: str, value: Union[int, float, str, bool, None]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.UserProfileSchema(
            user_id=user_id.bytes,
            name=name,
            value_bytes=pack_data(value),
            value_type=DataType.from_value(value).value
        )
        response = stub.CreateUserProfile(request=request, metadata=auth.metadata)
        return response.id

def update_user_profile(auth, id: int, name: Optional[str], value: Union[int, float, str, bool, None]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        value_bytes=None
        value_type=None
        if value != None: 
            value_bytes = pack_data(value)
            value_type = DataType.from_value(value).value
        request = profile_pb2.UserProfileUpdate(
            id=id,
            name=name,
            value_bytes=value_bytes,
            value_type=value_type
        )
        stub.UpdateUserProfile(request=request, metadata=auth.metadata)

def delete_user_profile(auth, id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.ProfileId(id=id)
        stub.DeleteUserProfile(request=request, metadata=auth.metadata)

def swap_user_profile(auth, user_id: UUID, name: str, order_1: int, order_2: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = profile_pb2_grpc.ProfileServiceStub(channel)
        request = profile_pb2.UserProfileSwap(
            user_id=user_id.bytes,
            name=name,
            order_1=order_1,
            order_2=order_2
        )
        stub.SwapUserProfile(request=request, metadata=auth.metadata)
