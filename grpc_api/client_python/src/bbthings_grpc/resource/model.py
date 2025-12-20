from ..proto.resource import model_pb2, model_pb2_grpc
from typing import Optional, Union, List
from uuid import UUID
import grpc
from ..common.type_value import DataType, pack_data
from ._schema import ModelSchema, ModelConfigSchema, TagSchema


def read_model(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelId(id=id.bytes)
        response = stub.ReadModel(request=request, metadata=resource.metadata)
        return ModelSchema.from_response(response.result)

def list_model_by_ids(resource, ids: list[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListModelByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelSchema.from_response(result))
        return ls

def list_model_by_type(resource, type_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.TypeId(id=type_id)
        response = stub.ListModelByType(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelSchema.from_response(result))
        return ls

def list_model_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelName(name=name)
        response = stub.ListModelByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelSchema.from_response(result))
        return ls

def list_model_by_category(resource, category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelCategory(category=category)
        response = stub.ListModelByCategory(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelSchema.from_response(result))
        return ls

def list_model_option(resource, type_id: Optional[UUID], name: Optional[str], category: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        type_bytes = None
        if type_id != None: type_bytes = type_id.bytes
        request = model_pb2.ModelOption(
            type_id=type_bytes,
            name=name,
            category=category
        )
        response = stub.ListModelOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelSchema.from_response(result))
        return ls

def create_model(resource, id: UUID, name: str, category: str, description: str, data_type: List[DataType]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        types = []
        for ty in data_type: types.append(ty.value)
        request = model_pb2.ModelSchema(
            id=id.bytes,
            name=name,
            category=category,
            description=description,
            data_type=types
        )
        response = stub.CreateModel(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_model(resource, id: UUID, name: Optional[str]=None, category: Optional[str]=None, description: Optional[str]=None, data_type: Optional[List[DataType]]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        type_flag = False
        types = []
        if (data_type is not None):
            type_flag = True
            for ty in data_type: types.append(ty.value)
        request = model_pb2.ModelUpdate(
            id=id.bytes,
            name=name,
            category=category,
            description=description,
            data_type=types,
            data_type_flag=type_flag
        )
        stub.UpdateModel(request=request, metadata=resource.metadata)

def delete_model(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelId(id=id.bytes)
        stub.DeleteModel(request=request, metadata=resource.metadata)

def read_model_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ConfigId(id=id)
        response = stub.ReadModelConfig(request=request, metadata=resource.metadata)
        return ModelConfigSchema.from_response(response.result)

def list_model_config_by_model(resource, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelId(id=model_id.bytes)
        response = stub.ListModelConfig(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(ModelConfigSchema.from_response(result))
        return ls

def create_model_config(resource, model_id: UUID, index: int, name: str, value: Union[int, float, str, bool, None], category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ConfigSchema(
            model_id=model_id.bytes,
            index=index,
            name=name,
            config_bytes=pack_data(value),
            config_type=DataType.from_value(value).value,
            category=category
        )
        response = stub.CreateModelConfig(request=request, metadata=resource.metadata)
        return response.id

def update_model_config(resource, id: int, name: Optional[str]=None, value: Union[int, float, str, bool, None]=None, category: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ConfigUpdate(
            id=id,
            name=name,
            config_bytes=pack_data(value),
            config_type=DataType.from_value(value).value,
            category=category
        )
        stub.UpdateModelConfig(request=request, metadata=resource.metadata)

def delete_model_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ConfigId(id=id)
        stub.DeleteModelConfig(request=request, metadata=resource.metadata)

def read_tag(resource, model_id: UUID, tag: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.TagId(
            model_id=model_id.bytes,
            tag=tag
        )
        response = stub.ReadTag(request=request, metadata=resource.metadata)
        return TagSchema.from_response(response.result)

def list_tag_by_model(resource, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.ModelId(id=model_id.bytes)
        response = stub.ListTagByModel(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(TagSchema.from_response(result))
        return ls

def create_tag(resource, model_id: UUID, tag: int, name: str, members: List[int]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.TagSchema(
            model_id=model_id.bytes,
            tag=tag,
            name=name,
            members=members
        )
        stub.CreateTag(request=request, metadata=resource.metadata)

def update_tag(resource, model_id: UUID, tag: int, name: Optional[str]=None, members: Optional[List[int]]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        flag = members is not None
        request = model_pb2.TagUpdate(
            model_id=model_id.bytes,
            tag=tag,
            name=name,
            members=members,
            members_flag=flag
        )
        stub.UpdateTag(request=request, metadata=resource.metadata)

def delete_tag(resource, model_id: UUID, tag: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = model_pb2_grpc.ModelServiceStub(channel)
        request = model_pb2.TagId(
            model_id=model_id.bytes,
            tag=tag
        )
        stub.DeleteTag(request=request, metadata=resource.metadata)
