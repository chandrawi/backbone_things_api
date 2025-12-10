from ..proto.auth import api_pb2, api_pb2_grpc, auth_pb2, auth_pb2_grpc
from typing import Optional, List
from uuid import UUID
import grpc
from ._schema import ApiSchema, ProcedureSchema
from ..common import utility


def read_api(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiId(id=id.bytes)
        response = stub.ReadApi(request=request, metadata=auth.metadata)
        return ApiSchema.from_response(response.result)

def read_api_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiName(name=name)
        response = stub.ReadApiByName(request=request, metadata=auth.metadata)
        return ApiSchema.from_response(response.result)

def list_api_by_ids(auth, ids: List[UUID]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListApiByIds(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ApiSchema.from_response(result))
        return ls

def list_api_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiName(name=name)
        response = stub.ListApiByName(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ApiSchema.from_response(result))
        return ls

def list_api_by_category(auth, category: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiCategory(category=category)
        response = stub.ListApiByCategory(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ApiSchema.from_response(result))
        return ls

def list_api_option(auth, name: Optional[str], category: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiOption(name=name, category=category)
        response = stub.ListApiOption(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ApiSchema.from_response(result))
        return ls

def create_api(auth, id: UUID, name: str, address: str, category: str, description: str, password: str, access_key: bytes):
    with grpc.insecure_channel(auth.address) as channel:
        stub = auth_pb2_grpc.AuthServiceStub(channel)
        request = auth_pb2.ApiKeyRequest()
        response = stub.ApiPasswordKey(request=request, metadata=auth.metadata)
        encrypted = utility.encrypt_message(password, response.public_key)
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiSchema(
            id=id.bytes,
            name=name,
            address=address,
            category=category,
            description=description,
            password=encrypted,
            access_key=access_key
        )
        response = stub.CreateApi(request=request, metadata=auth.metadata)
        return UUID(bytes=response.id)

def update_api(auth, id: UUID, name: Optional[str], address: Optional[str], category: Optional[str], description: Optional[str], password: Optional[str], access_key: Optional[bytes]):
    with grpc.insecure_channel(auth.address) as channel:
        encrypted = None
        if password != None:
            stub = auth_pb2_grpc.AuthServiceStub(channel)
            request = auth_pb2.ApiKeyRequest()
            response = stub.ApiPasswordKey(request=request, metadata=auth.metadata)
            encrypted = utility.encrypt_message(password, response.public_key)
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiUpdate(
            id=id.bytes,
            name=name,
            address=address,
            category=category,
            description=description,
            password=encrypted,
            access_key=access_key
        )
        stub.UpdateApi(request=request, metadata=auth.metadata)

def delete_api(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiId(id=id.bytes)
        stub.DeleteApi(request=request, metadata=auth.metadata)

def read_procedure(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureId(id=id.bytes)
        response = stub.ReadProcedure(request=request, metadata=auth.metadata)
        return ProcedureSchema.from_response(response.result)

def read_procedure_by_name(auth, api_id: UUID, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureName(api_id=api_id.bytes, name=name)
        response = stub.ReadProcedureByName(request=request, metadata=auth.metadata)
        return ProcedureSchema.from_response(response.result)

def list_procedure_by_ids(auth, ids: List[UUID]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListProcedureByIds(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ProcedureSchema.from_response(result))
        return ls

def list_procedure_by_api(auth, api_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ApiId(id=api_id.bytes)
        response = stub.ListProcedureByApi(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ProcedureSchema.from_response(result))
        return ls

def list_procedure_by_name(auth, name: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureName(name=name)
        response = stub.ListProcedureByName(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ProcedureSchema.from_response(result))
        return ls

def list_procedure_option(auth, api_id: Optional[UUID], name: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        api_bytes = None
        if api_id != None: api_bytes = api_id.bytes
        request = api_pb2.ProcedureOption(
            api_id=api_bytes,
            name=name
        )
        response = stub.ListProcedureOption(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(ProcedureSchema.from_response(result))
        return ls

def create_procedure(auth, id: UUID, api_id: UUID, name: str, description: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureSchema(
            id=id.bytes,
            api_id=api_id.bytes,
            name=name,
            description=description
        )
        response = stub.CreateProcedure(request=request, metadata=auth.metadata)
        return UUID(bytes=response.id)

def update_procedure(auth, id: UUID, name: Optional[str], description: Optional[str]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureUpdate(
            id=id.bytes,
            name=name,
            description=description
        )
        stub.UpdateProcedure(request=request, metadata=auth.metadata)

def delete_procedure(auth, id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = api_pb2_grpc.ApiServiceStub(channel)
        request = api_pb2.ProcedureId(id=id.bytes)
        stub.DeleteProcedure(request=request, metadata=auth.metadata)
