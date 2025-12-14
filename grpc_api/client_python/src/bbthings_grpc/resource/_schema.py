from typing import List, Union
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from ..common.type_value import DataType, unpack_data, unpack_data_array


@dataclass
class ProcedureAcces:
    procedure: str
    roles: List[str]

    def from_response(r):
        return ProcedureAcces(r.procedure, r.roles)


@dataclass
class RoleAcces:
    role: str
    procedures: List[str]

    def from_response(r):
        return RoleAcces(r.role, r.procedures)


@dataclass
class ModelConfigSchema:
    id: int
    model_id: UUID
    index: int
    name: str
    value: Union[int, float, str, bool, None]
    category: str

    def from_response(r):
        value = unpack_data(r.config_bytes, DataType(r.config_type))
        return ModelConfigSchema(r.id, UUID(bytes=r.model_id), r.index, r.name, value, r.category)


@dataclass
class ModelSchema:
    id: UUID
    category: str
    name: str
    description: str
    data_type: List[DataType]
    configs: List[List[ModelConfigSchema]]

    def from_response(r):
        types = []
        for ty in r.data_type:
            types.append(DataType(ty))
        configs = []
        for conf_vec in r.configs:
            confs = []
            for conf in conf_vec.configs: confs.append(ModelConfigSchema.from_response(conf))
            configs.append(confs)
        return ModelSchema(UUID(bytes=r.id), r.category, r.name, r.description, types, configs)


@dataclass
class TagSchema:
    model_id: UUID
    tag: int
    name: str
    members: List[int]

    def from_response(r):
        return TagSchema(UUID(bytes=r.model_id), r.tag, r.name, r.members)


@dataclass
class TypeConfigSchema:
    id: int
    type_id: UUID
    name: str
    value_type: DataType
    category: str

    def from_response(r):
        return TypeConfigSchema(r.id, UUID(bytes=r.type_id), r.name, DataType(r.config_type), r.category)


@dataclass
class TypeSchema:
    id: UUID
    name: str
    description: str
    model_ids: List[UUID]
    configs: List[TypeConfigSchema]

    def from_response(r):
        models = []
        for model in r.model_ids: models.append(UUID(bytes=model))
        configs = []
        for conf in r.configs: configs.append(TypeConfigSchema.from_response(conf))
        return TypeSchema(UUID(bytes=r.id), r.name, r.description, models)


@dataclass
class DeviceConfigSchema:
    id: int
    device_id: UUID
    name: str
    value: Union[int, float, str, bool, None]
    category: str

    def from_response(r):
        value = unpack_data(r.config_bytes, DataType(r.config_type))
        return DeviceConfigSchema(r.id, UUID(bytes=r.device_id), r.name, value, r.category)


@dataclass
class GatewayConfigSchema:
    id: int
    gateway_id: UUID
    name: str
    value: Union[int, float, str, bool, None]
    category: str

    def from_response(r):
        value = unpack_data(r.config_bytes, DataType(r.config_type))
        return GatewayConfigSchema(r.id, UUID(bytes=r.gateway_id), r.name, value, r.category)


@dataclass
class DeviceSchema:
    id: UUID
    gateway_id: UUID
    serial_number: str
    name: str
    description: str
    type_id: UUID
    type_name: UUID
    model_ids: List[UUID]
    configs: List[DeviceConfigSchema]

    def from_response(r):
        configs = []
        for conf in r.configs: configs.append(DeviceConfigSchema.from_response(conf))
        model_ids = []
        for id in r.model_ids: model_ids.append(UUID(bytes=id))
        return DeviceSchema(UUID(bytes=r.id), UUID(bytes=r.gateway_id), r.serial_number, r.name, r.description, r.type_id, r.type_name, model_ids, configs)


@dataclass
class GatewaySchema:
    id: UUID
    serial_number: str
    name: str
    description: str
    type_id: UUID
    type_name: UUID
    model_ids: List[UUID]
    configs: List[GatewayConfigSchema]

    def from_response(r):
        configs = []
        for conf in r.configs: configs.append(GatewayConfigSchema.from_response(conf))
        model_ids = []
        for id in r.model_ids: model_ids.append(UUID(bytes=id))
        return GatewaySchema(UUID(bytes=r.id), r.serial_number, r.name, r.description, r.type_id, r.type_name, model_ids, configs)


@dataclass
class GroupModelSchema:
    id: UUID
    name: str
    category: str
    description: str
    model_ids: List[UUID]

    def from_response(r):
        models = []
        for model in r.model_ids: models.append(UUID(bytes=model))
        return GroupModelSchema(UUID(bytes=r.id), r.name, r.category, r.description, models)


@dataclass
class GroupDeviceSchema:
    id: UUID
    name: str
    category: str
    description: str
    device_ids: List[UUID]

    def from_response(r):
        devices = []
        for device in r.device_ids: devices.append(UUID(bytes=device))
        return GroupDeviceSchema(UUID(bytes=r.id), r.name, r.category, r.description, devices)


@dataclass
class GroupGatewaySchema:
    id: UUID
    name: str
    category: str
    description: str
    gateway_ids: List[UUID]

    def from_response(r):
        gateways = []
        for gateway in r.device_ids: gateways.append(UUID(bytes=gateway))
        return GroupGatewaySchema(UUID(bytes=r.id), r.name, r.category, r.description, gateways)


@dataclass
class SetMember:
    device_id: UUID
    model_id: UUID
    data_index: List[int]

    def from_response(r):
        return SetMember(UUID(bytes=r.device_id), UUID(bytes=r.model_id), list(r.data_index))


@dataclass
class SetSchema:
    id: UUID
    template_id: UUID
    name: str
    description: str
    members: List[SetMember]

    def from_response(r):
        members = []
        for member in r.members: members.append(SetMember.from_response(member))
        return SetSchema(UUID(bytes=r.id), UUID(bytes=r.template_id), r.name, r.description, members)


@dataclass
class SetTemplateMember:
    type_id: UUID
    model_id: UUID
    data_index: List[int]

    def from_response(r):
        return SetTemplateMember(UUID(bytes=r.type_id), UUID(bytes=r.model_id), list(r.data_index))


@dataclass
class SetTemplateSchema:
    id: UUID
    name: str
    description: str
    members: List[SetTemplateMember]

    def from_response(r):
        members = []
        for member in r.members: members.append(SetTemplateMember.from_response(member))
        return SetTemplateSchema(UUID(bytes=r.id), r.name, r.description, members)


@dataclass
class DataSchema:
    device_id: UUID
    model_id: UUID
    timestamp: datetime
    data: List[Union[int, float, str, bool, None]]
    tag: int

    def from_response(r):
        timestamp = datetime.fromtimestamp(r.timestamp/1000000.0)
        types = []
        for ty in r.data_type: types.append(DataType(ty))
        data = unpack_data_array(r.data_bytes, types)
        return DataSchema(UUID(bytes=r.device_id), UUID(bytes=r.model_id), timestamp, data, r.tag)


@dataclass
class DataSetSchema:
    set_id: UUID
    timestamp: datetime
    data: List[Union[int, float, str, bool, None]]
    tag: int

    def from_response(r):
        timestamp = datetime.fromtimestamp(r.timestamp/1000000.0)
        types = []
        for ty in r.data_type: types.append(DataType(ty))
        data = unpack_data_array(r.data_bytes, types)
        return DataSetSchema(UUID(bytes=r.set_id), timestamp, data, r.tag)


@dataclass
class BufferSchema:
    id: int
    device_id: UUID
    model_id: UUID
    timestamp: datetime
    data: List[Union[int, float, str, bool, None]]
    tag: int

    def from_response(r):
        timestamp = datetime.fromtimestamp(r.timestamp/1000000.0)
        types = []
        for ty in r.data_type: types.append(DataType(ty))
        data = unpack_data_array(r.data_bytes, types)
        return BufferSchema(r.id, UUID(bytes=r.device_id), UUID(bytes=r.model_id), timestamp, data, r.tag)


@dataclass
class BufferSetSchema:
    ids: List[int]
    set_id: UUID
    timestamp: datetime
    data: List[Union[int, float, str, bool, None]]
    tag: int

    def from_response(r):
        timestamp = datetime.fromtimestamp(r.timestamp/1000000.0)
        types = []
        for ty in r.data_type: types.append(DataType(ty))
        data = unpack_data_array(r.data_bytes, types)
        return BufferSchema(r.ids, UUID(bytes=r.set_id), timestamp, data, r.tag)


@dataclass
class SliceSchema:
    id: int
    device_id: UUID
    model_id: UUID
    timestamp_begin: datetime
    timestamp_end: datetime
    name: str
    description: str

    def from_response(r):
        timestamp_begin = datetime.fromtimestamp(r.timestamp_begin/1000000.0)
        timestamp_end = datetime.fromtimestamp(r.timestamp_end/1000000.0)
        return SliceSchema(r.id, UUID(bytes=r.device_id), UUID(bytes=r.model_id), timestamp_begin, timestamp_end, r.name, r.description)


@dataclass
class SliceSetSchema:
    id: int
    set_id: UUID
    timestamp_begin: datetime
    timestamp_end: datetime
    name: str
    description: str

    def from_response(r):
        timestamp_begin = datetime.fromtimestamp(r.timestamp_begin/1000000.0)
        timestamp_end = datetime.fromtimestamp(r.timestamp_end/1000000.0)
        return SliceSetSchema(r.id, UUID(bytes=r.set_id), timestamp_begin, timestamp_end, r.name, r.description)
