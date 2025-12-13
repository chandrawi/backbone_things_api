from ..proto.resource import config_pb2, config_pb2_grpc
from uuid import UUID
from ._schema import ProcedureAcces, RoleAcces
import grpc


def api_id(resource):
    with grpc.insecure_channel(resource.address) as channel:
        stub = config_pb2_grpc.ConfigServiceStub(channel)
        request = config_pb2.ApiIdRequest()
        response = stub.ApiId(request=request, metadata=resource.metadata)
        return UUID(bytes=response.api_id)

def procedure_access(resource):
    with grpc.insecure_channel(resource.address) as channel:
        stub = config_pb2_grpc.ConfigServiceStub(channel)
        request = config_pb2.AccessRequest()
        response = stub.ProcedureAccess(request=request, metadata=resource.metadata)
        ls = []
        for result in response.access: ls.append(ProcedureAcces.from_response(result))
        return ls

def role_access(resource):
    with grpc.insecure_channel(resource.address) as channel:
        stub = config_pb2_grpc.ConfigServiceStub(channel)
        request = config_pb2.AccessRequest()
        response = stub.RoleAccess(request=request, metadata=resource.metadata)
        ls = []
        for result in response.access: ls.append(RoleAcces.from_response(result))
        return ls
