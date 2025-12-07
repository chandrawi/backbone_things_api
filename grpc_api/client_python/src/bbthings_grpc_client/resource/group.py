from bbthings_grpc_proto.resource import group_pb2, group_pb2_grpc
from typing import Optional, List
from uuid import UUID
import grpc
from ._schema import GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema


def read_group_model(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        response = stub.ReadGroupModel(request=request, metadata=resource.metadata)
        return GroupModelSchema.from_response(response.result)

def list_group_model_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListGroupModelByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupModelSchema.from_response(result))
        return ls

def list_group_model_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupName(name=name)
        response = stub.ListGroupModelByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupModelSchema.from_response(result))
        return ls

def list_group_model_by_category(resource, category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupCategory(category=category)
        response = stub.ListGroupModelByCategory(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupModelSchema.from_response(result))
        return ls

def list_group_model_option(resource, name: Optional[str], category: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupOption(name=name, category=category)
        response = stub.ListGroupModelOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupModelSchema.from_response(result))
        return ls

def create_group_model(resource, id: UUID, name: str, category: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupModelSchema(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        response = stub.CreateGroupModel(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_group_model(resource, id: UUID, name: Optional[str]=None, category: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupUpdate(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        stub.UpdateGroupModel(request=request, metadata=resource.metadata)

def delete_group_model(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        stub.DeleteGroupModel(request=request, metadata=resource.metadata)

def add_group_model_member(resource, id: UUID, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupModel(id=id.bytes, model_id=model_id.bytes)
        stub.AddGroupModelMember(request=request, metadata=resource.metadata)

def remove_group_model_member(resource, id: UUID, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupModel(id=id.bytes, model_id=model_id.bytes)
        stub.RemoveGroupModelMember(request=request, metadata=resource.metadata)

def read_group_device(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        response = stub.ReadGroupDevice(request=request, metadata=resource.metadata)
        return GroupDeviceSchema.from_response(response.result)

def list_group_device_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListGroupDeviceByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupDeviceSchema.from_response(result))
        return ls

def list_group_device_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupName(name=name)
        response = stub.ListGroupDeviceByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupDeviceSchema.from_response(result))
        return ls

def list_group_device_by_category(resource, category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupCategory(category=category)
        response = stub.ListGroupDeviceByCategory(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupDeviceSchema.from_response(result))
        return ls

def list_group_device_option(resource, name: Optional[str], category: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupOption(name=name, category=category)
        response = stub.ListGroupDeviceOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupDeviceSchema.from_response(result))
        return ls

def create_group_device(resource, id: UUID, name: str, category: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDeviceSchema(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        response = stub.CreateGroupDevice(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_group_device(resource, id: UUID, name: Optional[str]=None, category: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupUpdate(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        stub.UpdateGroupDevice(request=request, metadata=resource.metadata)

def delete_group_device(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        stub.DeleteGroupDevice(request=request, metadata=resource.metadata)

def add_group_device_member(resource, id: UUID, device_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDevice(id=id.bytes, device_id=device_id.bytes)
        stub.AddGroupDeviceMember(request=request, metadata=resource.metadata)

def remove_group_device_member(resource, id: UUID, device_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDevice(id=id.bytes, device_id=device_id.bytes)
        stub.RemoveGroupDeviceMember(request=request, metadata=resource.metadata)

def read_group_gateway(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        response = stub.ReadGroupGateway(request=request, metadata=resource.metadata)
        return GroupGatewaySchema.from_response(response.result)

def list_group_gateway_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListGroupGatewayByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupGatewaySchema.from_response(result))
        return ls

def list_group_gateway_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupName(name=name)
        response = stub.ListGroupGatewayByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupGatewaySchema.from_response(result))
        return ls

def list_group_gateway_by_category(resource, category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupCategory(category=category)
        response = stub.ListGroupGatewayByCategory(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupGatewaySchema.from_response(result))
        return ls

def list_group_gateway_option(resource, name: Optional[str], category: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupOption(name=name, category=category)
        response = stub.ListGroupGatewayOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GroupGatewaySchema.from_response(result))
        return ls

def create_group_gateway(resource, id: UUID, name: str, category: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDeviceSchema(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        response = stub.CreateGroupGateway(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_group_gateway(resource, id: UUID, name: Optional[str]=None, category: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupUpdate(
            id=id.bytes,
            name=name,
            category=category,
            description=description
        )
        stub.UpdateGroupGateway(request=request, metadata=resource.metadata)

def delete_group_gateway(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupId(id=id.bytes)
        stub.DeleteGroupGateway(request=request, metadata=resource.metadata)

def add_group_gateway_member(resource, id: UUID, gateway_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDevice(id=id.bytes, device_id=gateway_id.bytes)
        stub.AddGroupGatewayMember(request=request, metadata=resource.metadata)

def remove_group_gateway_member(resource, id: UUID, gateway_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = group_pb2_grpc.GroupServiceStub(channel)
        request = group_pb2.GroupDevice(id=id.bytes, device_id=gateway_id.bytes)
        stub.RemoveGroupGatewayMember(request=request, metadata=resource.metadata)
