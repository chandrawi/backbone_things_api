from ..proto.resource import device_pb2, device_pb2_grpc
from typing import Optional, Union, List
from uuid import UUID
import grpc
from ..common.type_value import DataType, pack_type, pack_data, pack_data_type
from ._schema import DeviceSchema, DeviceConfigSchema, GatewaySchema, GatewayConfigSchema, TypeSchema, TypeConfigSchema


def read_device(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceId(id=id.bytes)
        response = stub.ReadDevice(request=request, metadata=resource.metadata)
        return DeviceSchema.from_response(response.result)

def read_device_by_sn(resource, serial_number: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.SerialNumber(serial_number=serial_number)
        response = stub.ReadDeviceBySn(request=request, metadata=resource.metadata)
        return DeviceSchema.from_response(response.result)

def list_device_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListDeviceByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def list_device_by_gateway(resource, gateway_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayId(id=gateway_id.bytes)
        response = stub.ListDeviceByGateway(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def list_device_by_type(resource, type_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeId(id=type_id.bytes)
        response = stub.ListDeviceByType(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def list_device_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceName(name=name)
        response = stub.ListDeviceByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def list_device_option(resource, gateway_id: Optional[UUID], type_id: Optional[UUID], name: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        gateway_bytes = None
        if gateway_id != None: gateway_bytes = gateway_id.bytes
        type_bytes = None
        if type_id != None: type_bytes = type_id.bytes
        request = device_pb2.DeviceOption(
            gateway_id=gateway_bytes, 
            type_id=type_bytes,
            name=name
        )
        response = stub.ListDeviceOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def create_device(resource, id: UUID, gateway_id: UUID, type_id: UUID, serial_number: str, name: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceSchema(
            id=id.bytes,
            gateway_id=gateway_id.bytes,
            serial_number=serial_number,
            name=name,
            description=description,
            type_id=type_id.bytes
        )
        response = stub.CreateDevice(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_device(resource, id: UUID, gateway_id: Optional[UUID]=None, type_id: Optional[UUID]=None, serial_number: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        gateway_bytes = None
        if gateway_id != None: gateway_bytes = gateway_id.bytes
        type_bytes = None
        if type_id != None: type_bytes = type_id.bytes
        request = device_pb2.DeviceUpdate(
            id=id.bytes,
            gateway_id=gateway_bytes,
            serial_number=serial_number,
            name=name,
            description=description,
            type_id=type_bytes
        )
        stub.UpdateDevice(request=request, metadata=resource.metadata)

def delete_device(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceId(id=id.bytes)
        stub.DeleteDevice(request=request, metadata=resource.metadata)

def read_gateway(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayId(id=id.bytes)
        response = stub.ReadGateway(request=request, metadata=resource.metadata)
        return GatewaySchema.from_response(response.result)

def read_gateway_by_sn(resource, serial_number: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.SerialNumber(serial_number=serial_number)
        response = stub.ReadGatewayBySn(request=request, metadata=resource.metadata)
        return GatewaySchema.from_response(response.result)

def list_gateway_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListGatewayByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GatewaySchema.from_response(result))
        return ls

def list_gateway_by_type(resource, type_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeId(id=type_id.bytes)
        response = stub.ListGatewayByType(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GatewaySchema.from_response(result))
        return ls

def list_gateway_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayName(name=name)
        response = stub.ListGatewayByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GatewaySchema.from_response(result))
        return ls

def list_gateway_option(resource, type_id: Optional[UUID], name: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        type_bytes = None
        if type_id != None: type_bytes = type_id.bytes
        request = device_pb2.GatewayOption(
            type_id=type_bytes,
            name=name
        )
        response = stub.ListGatewayOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceSchema.from_response(result))
        return ls

def create_gateway(resource, id: UUID, type_id: UUID, serial_number: str, name: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewaySchema(
            id=id.bytes,
            serial_number=serial_number,
            name=name,
            description=description,
            type_id=type_id.bytes
        )
        response = stub.CreateGateway(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_gateway(resource, id: UUID, type_id: Optional[UUID]=None, serial_number: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        type_bytes = None
        if type_id != None: type_bytes = type_id.bytes
        request = device_pb2.GatewayUpdate(
            id=id.bytes,
            serial_number=serial_number,
            name=name,
            description=description,
            type_id=type_bytes
        )
        stub.UpdateGateway(request=request, metadata=resource.metadata)

def delete_gateway(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayId(id=id.bytes)
        stub.DeleteGateway(request=request, metadata=resource.metadata)

def read_device_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigId(id=id)
        response = stub.ReadDeviceConfig(request=request, metadata=resource.metadata)
        return DeviceConfigSchema.from_response(response.result)

def list_device_config_by_device(resource, device_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.DeviceId(id=device_id.bytes)
        response = stub.ListDeviceConfig(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DeviceConfigSchema.from_response(result))
        return ls

def create_device_config(resource, device_id: UUID, name: str, value: Union[int, float, str, bool, None], category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigSchema(
            device_id=device_id.bytes,
            name=name,
            config_bytes=pack_data(value),
            config_type=pack_type(value),
            category=category
        )
        response = stub.CreateDeviceConfig(request=request, metadata=resource.metadata)
        return response.id

def update_device_config(resource, id: int, name: Optional[str]=None, value: Union[int, float, str, bool, None]=None, category: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigUpdate(
            id=id,
            name=name,
            config_bytes=pack_data(value),
            config_type=pack_type(value),
            category=category
        )
        stub.UpdateDeviceConfig(request=request, metadata=resource.metadata)

def delete_device_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigId(id=id)
        stub.DeleteDeviceConfig(request=request, metadata=resource.metadata)

def read_gateway_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigId(id=id)
        response = stub.ReadGatewayConfig(request=request, metadata=resource.metadata)
        return GatewayConfigSchema.from_response(response.result)

def list_gateway_config_by_gateway(resource, gateway_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.GatewayId(id=gateway_id.bytes)
        response = stub.ListGatewayConfig(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(GatewayConfigSchema.from_response(result))
        return ls

def create_gateway_config(resource, gateway_id: UUID, name: str, value: Union[int, float, str, bool, None], category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigSchema(
            device_id=gateway_id.bytes,
            name=name,
            config_bytes=pack_data(value),
            config_type=pack_type(value),
            category=category
        )
        response = stub.CreateGatewayConfig(request=request, metadata=resource.metadata)
        return response.id

def update_gateway_config(resource, id: int, name: Optional[str]=None, value: Union[int, float, str, bool, None]=None, category: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigUpdate(
            id=id,
            name=name,
            config_bytes=pack_data(value),
            config_type=pack_type(value),
            category=category
        )
        stub.UpdateGatewayConfig(request=request, metadata=resource.metadata)

def delete_gateway_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.ConfigId(id=id)
        stub.DeleteGatewayConfig(request=request, metadata=resource.metadata)

def read_type(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeId(id=id.bytes)
        response = stub.ReadType(request=request, metadata=resource.metadata)
        return TypeSchema.from_response(response.result)

def list_type_by_ids(resource, ids: List[UUID]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeIds(ids=list(map(lambda x: x.bytes, ids)))
        response = stub.ListTypeByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(TypeSchema.from_response(result))
        return ls

def list_type_by_name(resource, name: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeName(name=name)
        response = stub.ListTypeByName(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(TypeSchema.from_response(result))
        return ls

def list_type_option(resource, name: Optional[str]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeOption(name=name)
        response = stub.ListTypeOption(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(TypeSchema.from_response(result))
        return ls

def create_type(resource, id: UUID, name: str, description: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeSchema(
            id=id.bytes,
            name=name,
            description=description
        )
        response = stub.CreateType(request=request, metadata=resource.metadata)
        return UUID(bytes=response.id)

def update_type(resource, id: UUID, name: Optional[str]=None, description: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeUpdate(
            id=id.bytes,
            name=name,
            description=description
        )
        stub.UpdateType(request=request, metadata=resource.metadata)

def delete_type(resource, id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeId(id=id.bytes)
        stub.DeleteType(request=request, metadata=resource.metadata)

def add_type_model(resource, id: UUID, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeModel(id=id.bytes, model_id=model_id.bytes)
        stub.AddTypeModel(request=request, metadata=resource.metadata)

def remove_type_model(resource, id: UUID, model_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeModel(id=id.bytes, model_id=model_id.bytes)
        stub.RemoveTypeModel(request=request, metadata=resource.metadata)

def read_type_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeConfigId(id=id)
        response = stub.ReadTypeConfig(request=request, metadata=resource.metadata)
        return TypeConfigSchema.from_response(response.result)

def list_type_config_by_type(resource, type_id: UUID):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeId(id=type_id.bytes)
        response = stub.ListTypeConfig(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(TypeConfigSchema.from_response(result))
        return ls

def create_type_config(resource, type_id: UUID, name: str, value_type: DataType, value_default: Union[int, float, str, bool, None], category: str):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeConfigSchema(
            type_id=type_id.bytes,
            name=name,
            config_type=value_type.value,
            config_bytes=pack_data_type(value_default, value_type),
            category=category
        )
        response = stub.CreateTypeConfig(request=request, metadata=resource.metadata)
        return response.id

def update_type_config(resource, id: int, name: Optional[str]=None, value_type: Optional[DataType]=None, value_default: Union[int, float, str, bool, None]=None, category: Optional[str]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        ty = None
        if value_type != None: ty = value_type.value
        byt = None
        if value_type != None and value_default != None: byt = pack_data_type(value_default, value_type)
        elif value_default != None: byt = pack_data(value_default)
        request = device_pb2.TypeConfigUpdate(
            id=id,
            name=name,
            config_type=ty,
            config_bytes=byt,
            category=category
        )
        stub.UpdateTypeConfig(request=request, metadata=resource.metadata)

def delete_type_config(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = device_pb2_grpc.DeviceServiceStub(channel)
        request = device_pb2.TypeConfigId(id=id)
        stub.DeleteTypeConfig(request=request, metadata=resource.metadata)
