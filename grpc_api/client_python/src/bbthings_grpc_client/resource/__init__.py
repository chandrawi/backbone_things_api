from datetime import datetime
from uuid import UUID
from typing import Optional, Union, List
from . import model, device, group, set, data, buffer, slice
from ..common.type_value import DataType
from ._schema import ModelSchema, TagSchema, ModelConfigSchema, DeviceSchema, GatewaySchema, \
    TypeSchema, DeviceConfigSchema, GatewayConfigSchema, \
    GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema, SetSchema, SetTemplateSchema, \
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema, SliceSchema, SliceSetSchema


class Resource:

    def __init__(self, address: str, access_token: Optional[str] = None):
        self.address = address
        self.metadata = [] if access_token == None \
            else [("authorization", "Bearer " + access_token)]

    def read_model(self, id: UUID) -> ModelSchema:
        return model.read_model(self, id)

    def list_model_by_ids(self, ids: list[UUID]) -> List[ModelSchema]:
        return model.list_model_by_ids(self, ids)

    def list_model_by_type(self, type_id: UUID) -> List[ModelSchema]:
        return model.list_model_by_type(self, type_id)

    def list_model_by_name(self, name: str) -> List[ModelSchema]:
        return model.list_model_by_name(self, name)

    def list_model_by_category(self, category: str) -> List[ModelSchema]:
        return model.list_model_by_category(self, category)

    def list_model_option(self, type_id: Optional[UUID], name: Optional[str], category: Optional[str]) -> List[ModelSchema]:
        return model.list_model_option(self, type_id, name, category)

    def create_model(self, id: UUID, data_type: List[DataType], category: str, name: str, description: str) -> UUID:
        return model.create_model(self, id, data_type, category, name, description)

    def update_model(self, id: UUID, data_type: Optional[List[DataType]], category: Optional[str], name: Optional[str], description: Optional[str]):
        return model.update_model(self, id, data_type, category, name, description)

    def delete_model(self, id: UUID):
        return model.delete_model(self, id)

    def read_model_config(self, id: int) -> ModelConfigSchema:
        return model.read_model_config(self, id)

    def list_model_config_by_model(self, model_id: UUID) -> List[ModelConfigSchema]:
        return model.list_model_config_by_model(self, model_id)

    def create_model_config(self, model_id: UUID, index: int, name: str, value: Union[int, float, str, bool, None], category: str) -> int:
        return model.create_model_config(self, model_id, index, name, value, category)

    def update_model_config(self, id: int, name: Optional[str], value: Union[int, float, str, bool, None], category: Optional[str]):
        return model.update_model_config(self, id, name, value, category)

    def delete_model_config(self, id: int):
        return model.delete_model_config(self, id)

    def read_tag(self, model_id: UUID, tag: int) -> TagSchema:
        return model.read_tag(self, model_id, tag)

    def list_tag_by_model(self, model_id: UUID) -> List[TagSchema]:
        return model.list_tag_by_model(self, model_id)

    def create_tag(self, model_id: UUID, tag: int, name: str, members: List[int]) -> int:
        return model.create_tag(self, model_id, tag, name, members)

    def update_tag(self, model_id:UUID, tag: int, name: Optional[str], members: Optional[List[int]]):
        return model.update_tag(self, model_id, tag, name, members)

    def delete_tag(self, model_id:UUID, tag: int):
        return model.delete_tag(self, model_id, tag)

    def read_device(self, id: UUID) -> DeviceSchema:
        return device.read_device(self, id)

    def read_device_by_sn(self, serial_number: str) -> DeviceSchema:
        return device.read_device_by_sn(self, serial_number)

    def list_device_by_ids(self, ids: list[UUID]) -> List[DeviceSchema]:
        return device.list_device_by_ids(self, ids)

    def list_device_by_gateway(self, gateway_id: UUID) -> List[DeviceSchema]:
        return device.list_device_by_gateway(self, gateway_id)

    def list_device_by_type(self, type_id: UUID) -> List[DeviceSchema]:
        return device.list_device_by_type(self, type_id)

    def list_device_by_name(self, name: str) -> List[DeviceSchema]:
        return device.list_device_by_name(self, name)

    def list_device_option(self, gateway_id: Optional[UUID], type_id: Optional[UUID], name: Optional[str]) -> List[DeviceSchema]:
        return device.list_device_option(self, gateway_id, type_id, name)

    def create_device(self, id: UUID, gateway_id: UUID, type_id: UUID, serial_number: str, name: str, description: str) -> UUID:
        return device.create_device(self, id, gateway_id, type_id, serial_number, name, description)

    def update_device(self, id: UUID, gateway_id: Optional[UUID], type_id: Optional[UUID], serial_number: Optional[str], name: Optional[str], description: Optional[str]):
        return device.update_device(self, id, gateway_id, type_id, serial_number, name, description)

    def delete_device(self, id: UUID):
        return device.delete_device(self, id)

    def read_gateway(self, id: UUID) -> GatewaySchema:
        return device.read_gateway(self, id)

    def read_gateway_by_sn(self, serial_number: str) -> GatewaySchema:
        return device.read_gateway_by_sn(self, serial_number)

    def list_gateway_by_ids(self, ids: list[UUID]) -> List[GatewaySchema]:
        return device.list_gateway_by_ids(self, ids)

    def list_gateway_by_type(self, type_id: UUID) -> List[GatewaySchema]:
        return device.list_gateway_by_type(self, type_id)

    def list_gateway_by_name(self, name: str) -> List[GatewaySchema]:
        return device.list_gateway_by_name(self, name)

    def list_gateway_option(self, type_id: Optional[UUID], name: Optional[str]) -> List[GatewaySchema]:
        return device.list_gateway_option(self, type_id, name)

    def create_gateway(self, id: UUID, type_id: UUID, serial_number: str, name: str, description: str) -> UUID:
        return device.create_gateway(self, id, type_id, serial_number, name, description)

    def update_gateway(self, id: UUID, type_id: Optional[UUID], serial_number: Optional[str], name: Optional[str], description: Optional[str]):
        return device.update_gateway(self, id, type_id, serial_number, name, description)

    def delete_gateway(self, id: UUID):
        return device.delete_gateway(self, id)

    def read_device_config(self, id: int) -> DeviceConfigSchema:
        return device.read_device_config(self, id)

    def list_device_config_by_device(self, device_id: UUID) -> List[DeviceConfigSchema]:
        return device.list_device_config_by_device(self, device_id)

    def create_device_config(self, device_id: UUID, name: str, value: Union[int, float, str, bool, None], category: str) -> int:
        return device.create_device_config(self, device_id, name, value, category)

    def update_device_config(self, id: int, name: Optional[str], value: Union[int, float, str, bool, None], category: Optional[str]):
        return device.update_device_config(self, id, name, value, category)

    def delete_device_config(self, id: int):
        return device.delete_device_config(self, id)

    def read_gateway_config(self, id: int) -> GatewayConfigSchema:
        return device.read_gateway_config(self, id)

    def list_gateway_config_by_gateway(self, gateway_id: UUID) -> List[GatewayConfigSchema]:
        return device.list_gateway_config_by_gateway(self, gateway_id)

    def create_gateway_config(self, gateway_id: UUID, name: str, value: Union[int, float, str, bool, None], category: str) -> int:
        return device.create_gateway_config(self, gateway_id, name, value, category)

    def update_gateway_config(self, id: int, name: Optional[str], value: Union[int, float, str, bool, None], category: Optional[str]):
        return device.update_gateway_config(self, id, name, value, category)

    def delete_gateway_config(self, id: int):
        return device.delete_gateway_config(self, id)

    def read_type(self, id: UUID) -> TypeSchema:
        return device.read_type(self, id)

    def list_type_by_ids(self, ids: list[UUID]) -> List[TypeSchema]:
        return device.list_type_by_ids(self, ids)

    def list_type_by_name(self, name: str) -> List[TypeSchema]:
        return device.list_type_by_name(self, name)

    def list_type_option(self, name: Optional[str]) -> List[TypeSchema]:
        return device.list_type_option(self, name)

    def create_type(self, id: UUID, name: str, description: str) -> UUID:
        return device.create_type(self, id, name, description)

    def update_type(self, id: UUID, name: Optional[str], description: Optional[str]):
        return device.update_type(self, id, name, description)

    def delete_type(self, id: UUID):
        return device.delete_type(self, id)

    def add_type_model(self, id: UUID, model_id: UUID):
        return device.add_type_model(self, id, model_id)

    def remove_type_model(self, id: UUID, model_id: UUID):
        return device.remove_type_model(self, id, model_id)

    def read_group_model(self, id: UUID) -> GroupModelSchema:
        return group.read_group_model(self, id)

    def list_group_model_by_ids(self, ids: list[UUID]) -> List[GroupModelSchema]:
        return group.list_group_model_by_ids(self, ids)

    def list_group_model_by_name(self, name: str) -> List[GroupModelSchema]:
        return group.list_group_model_by_name(self, name)

    def list_group_model_by_category(self, category: str) -> List[GroupModelSchema]:
        return group.list_group_model_by_category(self, category)

    def list_group_model_option(self, name: Optional[str], category: Optional[str]) -> List[GroupModelSchema]:
        return group.list_group_model_option(self, name, category)

    def create_group_model(self, id: UUID, name: str, category: str, description: str) -> UUID:
        return group.create_group_model(self, id, name, category, description)

    def update_group_model(self, id: UUID, name: Optional[str], category: Optional[str], description: Optional[str]):
        return group.update_group_model(self, id, name, category, description)

    def delete_group_model(self, id: UUID):
        return group.delete_group_model(self, id)

    def add_group_model_member(self, id: UUID, model_id: UUID):
        return group.add_group_model_member(self, id, model_id)

    def remove_group_model_member(self, id: UUID, model_id: UUID):
        return group.remove_group_model_member(self, id, model_id)

    def read_group_device(self, id: UUID) -> GroupDeviceSchema:
        return group.read_group_device(self, id)

    def list_group_device_by_ids(self, ids: list[UUID]) -> List[GroupDeviceSchema]:
        return group.list_group_device_by_ids(self, ids)

    def list_group_device_by_name(self, name: str) -> List[GroupDeviceSchema]:
        return group.list_group_device_by_name(self, name)

    def list_group_device_by_category(self, category: str) -> List[GroupDeviceSchema]:
        return group.list_group_device_by_category(self, category)

    def list_group_device_option(self, name: Optional[str], category: Optional[str]) -> List[GroupDeviceSchema]:
        return group.list_group_device_option(self, name, category)

    def create_group_device(self, id: UUID, name: str, category: str, description: str) -> UUID:
        return group.create_group_device(self, id, name, category, description)

    def update_group_device(self, id: UUID, name: Optional[str], category: Optional[str], description: Optional[str]):
        return group.update_group_device(self, id, name, category, description)

    def delete_group_device(self, id: UUID):
        return group.delete_group_device(self, id)

    def add_group_device_member(self, id: UUID, device_id: UUID):
        return group.add_group_device_member(self, id, device_id)

    def remove_group_device_member(self, id: UUID, device_id: UUID):
        return group.remove_group_device_member(self, id, device_id)

    def read_group_gateway(self, id: UUID) -> GroupGatewaySchema:
        return group.read_group_gateway(self, id)

    def list_group_gateway_by_ids(self, ids: list[UUID]) -> List[GroupGatewaySchema]:
        return group.list_group_gateway_by_ids(self, ids)

    def list_group_gateway_by_name(self, name: str) -> List[GroupGatewaySchema]:
        return group.list_group_gateway_by_name(self, name)

    def list_group_gateway_by_category(self, category: str) -> List[GroupGatewaySchema]:
        return group.list_group_gateway_by_category(self, category)

    def list_group_gateway_option(self, name: Optional[str], category: Optional[str]) -> List[GroupGatewaySchema]:
        return group.list_group_gateway_option(self, name, category)

    def create_group_gateway(self, id: UUID, name: str, category: str, description: str) -> UUID:
        return group.create_group_gateway(self, id, name, category, description)

    def update_group_gateway(self, id: UUID, name: Optional[str], category: Optional[str], description: Optional[str]):
        return group.update_group_gateway(self, id, name, category, description)

    def delete_group_gateway(self, id: UUID):
        return group.delete_group_gateway(self, id)

    def add_group_gateway_member(self, id: UUID, gateway_id: UUID):
        return group.add_group_gateway_member(self, id, gateway_id)

    def remove_group_gateway_member(self, id: UUID, gateway_id: UUID):
        return group.remove_group_gateway_member(self, id, gateway_id)

    def read_set(self, id: UUID) -> SetSchema:
        return set.read_set(self, id)

    def list_set_by_ids(self, ids: List[UUID]) -> List[SetSchema]:
        return set.list_set_by_ids(self, ids)

    def list_set_by_template(self, template_id: UUID) -> List[SetSchema]:
        return set.list_set_by_template(self, template_id)

    def list_set_by_name(self, name: str) -> List[SetSchema]:
        return set.list_set_by_name(self, name)

    def list_set_option(self, template_id: Optional[UUID], name: Optional[UUID]) -> List[SetSchema]:
        return set.list_set_option(self, template_id, name)

    def create_set(self, id: UUID, template_id: UUID, name: str, description: str) -> UUID:
        return set.create_set(self, id, template_id, name, description)

    def update_set(self, id: UUID, template_id: Optional[UUID], name: Optional[str], description: Optional[str]):
        return set.update_set(self, id, template_id, name, description)

    def delete_set(self, id: UUID):
        return set.delete_set(self, id)

    def add_set_member(self, id: UUID, device_id: UUID, model_id: UUID, data_index: List[int]):
        return set.add_set_member(self, id, device_id, model_id, data_index)

    def remove_set_member(self, id: UUID, device_id: UUID, model_id: UUID):
        return set.remove_set_member(self, id, device_id, model_id)

    def swap_set_member(self, id: UUID, device_id_1: UUID, model_id_1: UUID, device_id_2: UUID, model_id_2: UUID):
        return set.swap_set_member(self, id, device_id_1, model_id_1, device_id_2, model_id_2)

    def read_set_template(self, id: UUID) -> SetTemplateSchema:
        return set.read_set_template(self, id)

    def list_set_template_by_ids(self, ids: List[UUID]) -> List[SetTemplateSchema]:
        return set.list_set_template_by_ids(self, ids)

    def list_set_template_by_name(self, name: str) -> List[SetTemplateSchema]:
        return set.list_set_template_by_name(self, name)

    def list_set_template_option(self, name: Optional[str]) -> List[SetTemplateSchema]:
        return set.list_set_template_option(self, name)

    def create_set_template(self, id: UUID, name: str, description: str) -> UUID:
        return set.create_set_template(self, id, name, description)

    def update_set_template(self, id: UUID, name: Optional[str], description: Optional[str]):
        return set.update_set_template(self, id, name, description)

    def delete_set_template(self, id: UUID):
        return set.delete_set_template(self, id)

    def add_set_template_member(self, id: UUID, type_id: UUID, model_id: UUID, data_index: List[int]):
        return set.add_set_template_member(self, id, type_id, model_id, data_index)

    def remove_set_template_member(self, id: UUID, index: int):
        return set.remove_set_template_member(self, id, index)

    def swap_set_template_member(self, id: UUID, index_1: int, index_2: int):
        return set.swap_set_template_member(self, id, index_1, index_2)

    def read_data(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> DataSchema:
        return data.read_data(self, device_id, model_id, timestamp, tag)

    def list_data_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_time(self, device_id, model_id, timestamp, tag)

    def list_data_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_earlier(self, device_id, model_id, earlier, tag)

    def list_data_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_later(self, device_id, model_id, later, tag)

    def list_data_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_range(self, device_id, model_id, begin, end, tag)

    def list_data_by_number_before(self, device_id: UUID, model_id: UUID, before: datetime, number: int, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_number_before(self, device_id, model_id, before, number, tag)

    def list_data_by_number_after(self, device_id: UUID, model_id: UUID, after: datetime, number: int, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_by_number_after(self, device_id, model_id, after, number, tag)

    def list_data_group_by_time(self, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_time(self, device_ids, model_ids, timestamp, tag)

    def list_data_group_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_earlier(self, device_ids, model_ids, earlier, tag)

    def list_data_group_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_later(self, device_ids, model_ids, later, tag)

    def list_data_group_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_range(self, device_ids, model_ids, begin, end, tag)

    def list_data_group_by_number_before(self, device_ids: List[UUID], model_ids: List[UUID], before: datetime, number: int, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_number_before(self, device_ids, model_ids, before, number, tag)

    def list_data_group_by_number_after(self, device_ids: List[UUID], model_ids: List[UUID], after: datetime, number: int, tag: Optional[int]=None) -> List[DataSchema]:
        return data.list_data_group_by_number_after(self, device_ids, model_ids, after, number, tag)

    def read_data_set(self, set_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> DataSetSchema:
        return data.read_data_set(self, set_id, timestamp, tag)

    def list_data_set_by_time(self, set_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> List[DataSetSchema]:
        return data.list_data_set_by_time(self, set_id, timestamp, tag)

    def list_data_set_by_earlier(self, set_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[DataSetSchema]:
        return data.list_data_set_by_earlier(self, set_id, earlier, tag)

    def list_data_set_by_later(self, set_id: UUID, later: datetime, tag: Optional[int]=None) -> List[DataSetSchema]:
        return data.list_data_set_by_later(self, set_id, later, tag)

    def list_data_set_by_range(self, set_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[DataSetSchema]:
        return data.list_data_set_by_range(self, set_id, begin, end, tag)

    def create_data(self, device_id: UUID, model_id: UUID, timestamp: datetime, data: List[Union[int, float, str, bool, None]], tag: Optional[int]=None):
        return data.create_data(self, device_id, model_id, timestamp, data, tag)

    def create_data_multiple(self, device_ids: list[UUID], model_ids: list[UUID], timestamps: list[datetime], data: List[List[Union[int, float, str, bool, None]]], tags:Optional[List[int]]=None):
        return data.create_data_multiple(self, device_ids, model_ids, timestamps, data, tags)

    def delete_data(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
        return data.delete_data(self, device_id, model_id, timestamp, tag)

    def read_data_timestamp(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> datetime:
        return data.read_data_timestamp(self, device_id, model_id, timestamp, tag)

    def list_data_timestamp_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_timestamp_by_earlier(self, device_id, model_id, earlier, tag)

    def list_data_timestamp_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_timestamp_by_later(self, device_id, model_id, later, tag)

    def list_data_timestamp_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_timestamp_by_range(self, device_id, model_id, begin, end, tag)

    def read_data_group_timestamp(self, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None) -> datetime:
        return data.read_data_group_timestamp(self, device_ids, model_ids, timestamp, tag)

    def list_data_group_timestamp_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_group_timestamp_by_earlier(self, device_ids, model_ids, earlier, tag)

    def list_data_group_timestamp_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_group_timestamp_by_later(self, device_ids, model_ids, later, tag)

    def list_data_group_timestamp_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> List[datetime]:
        return data.list_data_group_timestamp_by_range(self, device_ids, model_ids, begin, end, tag)

    def count_data(self, device_id: UUID, model_id: UUID, tag: Optional[int]=None) -> int:
        return data.count_data(self, device_id, model_id, tag)

    def count_data_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_by_earlier(self, device_id, model_id, earlier, tag)

    def count_data_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_by_later(self, device_id, model_id, later, tag)

    def count_data_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_by_range(self, model_id, device_id, begin, end, tag)

    def count_data_group(self, device_ids: List[UUID], model_ids: List[UUID], tag: Optional[int]=None) -> int:
        return data.count_data_group(self, device_ids, model_ids, tag)

    def count_data_group_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_group_by_earlier(self, device_ids, model_ids, earlier, tag)

    def count_data_group_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_group_by_later(self, device_ids, model_ids, later, tag)

    def count_data_group_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> int:
        return data.count_data_group_by_range(self, device_ids, model_ids, begin, end, tag)

    def read_buffer(self, id: int) -> BufferSchema:
        return buffer.read_buffer(self, id)

    def read_buffer_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> BufferSchema:
        return buffer.read_buffer_by_time(self, device_id, model_id, timestamp, tag)

    def list_buffer_by_ids(self, ids: List[int]) -> BufferSchema:
        return buffer.list_buffer_by_ids(self, ids)

    def list_buffer_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_time(self, device_id, model_id, timestamp, tag)

    def list_buffer_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_earlier(self, device_id, model_id, earlier, tag)

    def list_buffer_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_later(self, device_id, model_id, later, tag)

    def list_buffer_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_range(self, device_id, model_id, begin, end, tag)

    def list_buffer_by_number_before(self, device_id: UUID, model_id: UUID, before: datetime, number: int, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_number_before(self, device_id, model_id, before, number, tag)

    def list_buffer_by_number_after(self, device_id: UUID, model_id: UUID, after: datetime, number: int, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_by_number_after(self, device_id, model_id, after, number, tag)

    def read_buffer_first(self, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> BufferSchema:
        return buffer.read_buffer_first(self, device_id, model_id, tag)

    def read_buffer_last(self, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> BufferSchema:
        return buffer.read_buffer_last(self, device_id, model_id, tag)

    def list_buffer_first(self, number: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_first(self, number, device_id, model_id, tag)

    def list_buffer_first_offset(self, number: int, offset: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_first_offset(self, number, offset, device_id, model_id, tag)

    def list_buffer_last(self, number: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_last(self, number, device_id, model_id, tag)

    def list_buffer_last_offset(self, number: int, offset: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_last_offset(self, number, offset, device_id, model_id, tag)

    def list_buffer_group_by_time(self, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_time(self, device_ids, model_ids, timestamp, tag)

    def list_buffer_group_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_earlier(self, device_ids, model_ids, earlier, tag)

    def list_buffer_group_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_later(self, device_ids, model_ids, later, tag)

    def list_buffer_group_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_range(self, device_ids, model_ids, begin, end, tag)

    def list_buffer_group_by_number_before(self, device_ids: List[UUID], model_ids: List[UUID], before: datetime, number: int, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_number_before(self, device_ids, model_ids, before, number, tag)

    def list_buffer_group_by_number_after(self, device_ids: List[UUID], model_ids: List[UUID], after: datetime, number: int, tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_by_number_after(self, device_ids, model_ids, after, number, tag)

    def read_buffer_group_first(self, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.read_buffer_group_first(self, device_ids, model_ids, tag)

    def read_buffer_group_last(self, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.read_buffer_group_last(self, device_ids, model_ids, tag)

    def list_buffer_group_first(self, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_first(self, number, device_ids, model_ids, tag)

    def list_buffer_group_first_offset(self, number: int, offset: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_first_offset(self, number, offset, device_ids, model_ids, tag)

    def list_buffer_group_last(self, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_last(self, number, device_ids, model_ids, tag)

    def list_buffer_group_last_offset(self, number: int, offset: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[BufferSchema]:
        return buffer.list_buffer_group_last_offset(self, number, offset, device_ids, model_ids, tag)

    def read_buffer_set(self, set_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> BufferSetSchema:
        return buffer.read_buffer_set(self, set_id, timestamp, tag)

    def list_buffer_set_by_time(self, set_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> List[BufferSetSchema]:
        return buffer.list_buffer_set_by_time(self, set_id, timestamp, tag)

    def list_buffer_set_by_earlier(self, set_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[BufferSetSchema]:
        return buffer.list_buffer_set_by_earlier(self, set_id, earlier, tag)

    def list_buffer_set_by_later(self, set_id: UUID, later: datetime, tag: Optional[int]=None) -> List[BufferSetSchema]:
        return buffer.list_buffer_set_by_later(self, set_id, later, tag)

    def list_buffer_set_by_range(self, set_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[BufferSetSchema]:
        return buffer.list_buffer_set_by_range(self, set_id, begin, end, tag)

    def create_buffer(self, device_id: UUID, model_id: UUID, timestamp: datetime, data: List[Union[int, float, str, bool, None]], tag: Optional[int]=None) -> int:
        return buffer.create_buffer(self, device_id, model_id, timestamp, data, tag)

    def create_buffer_multiple(self, device_ids: list[UUID], model_ids: list[UUID], timestamps: list[datetime], data: List[List[Union[int, float, str, bool, None]]], tags: Optional[List[int]]=None) -> List[int]:
        return buffer.create_buffer_multiple(self, device_ids, model_ids, timestamps, data, tags)

    def update_buffer(self, id: int, data: Optional[List[Union[int, float, str, bool, None]]]=None, tag: Optional[Union[int]]=None):
        return buffer.update_buffer(self, id, data, tag)

    def update_buffer_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime, data: Optional[List[Union[int, float, str, bool, None]]]=None, tag: Optional[Union[int]]=None):
        return buffer.update_buffer_by_time(self, device_id, model_id, timestamp, data, tag)

    def delete_buffer(self, id: int):
        return buffer.delete_buffer(self, id)

    def delete_buffer_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[Union[int]]=None):
        return buffer.delete_buffer_by_time(self, device_id, model_id, timestamp, tag)

    def read_buffer_timestamp(self, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None) -> datetime:
        return buffer.read_buffer_timestamp(self, device_id, model_id, timestamp, tag)

    def list_buffer_timestamp_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_timestamp_by_earlier(self, device_id, model_id, earlier, tag)

    def list_buffer_timestamp_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_timestamp_by_later(self, device_id, model_id, later, tag)

    def list_buffer_timestamp_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_timestamp_by_range(self, device_id, model_id, begin, end, tag)

    def list_buffer_timestamp_first(self, number: int, device_id: Optional[UUID], model_id: Optional[UUID], tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_timestamp_first(self, number, device_id, model_id, tag)

    def list_buffer_timestamp_last(self, number: int, device_id: Optional[UUID], model_id: Optional[UUID], tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_timestamp_last(self, number, device_id, model_id, tag)

    def read_buffer_group_timestamp(self, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None) -> datetime:
        return buffer.read_buffer_group_timestamp(self, device_ids, model_ids, timestamp, tag)

    def list_buffer_group_timestamp_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_group_timestamp_by_earlier(self, device_ids, model_ids, earlier, tag)

    def list_buffer_group_timestamp_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_group_timestamp_by_later(self, device_ids, model_ids, later, tag)

    def list_buffer_group_timestamp_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_group_timestamp_by_range(self, device_ids, model_ids, begin, end, tag)

    def list_buffer_group_timestamp_first(self, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_group_timestamp_first(self, number, device_ids, model_ids, tag)

    def list_buffer_group_timestamp_last(self, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None) -> List[datetime]:
        return buffer.list_buffer_group_timestamp_last(self, number, device_ids, model_ids, tag)

    def count_buffer(self, device_id: UUID, model_id: UUID, tag: Optional[int]=None) -> int:
        return buffer.count_buffer(self, device_id, model_id, tag)

    def count_buffer_by_earlier(self, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_by_earlier(self, device_id, model_id, earlier, tag)

    def count_buffer_by_later(self, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_by_later(self, device_id, model_id, later, tag)

    def count_buffer_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_by_range(self, device_id, model_id, begin, end, tag)

    def count_buffer_group(self, device_ids: List[UUID], model_ids: List[UUID], tag: Optional[int]=None) -> int:
        return buffer.count_buffer_group(self, device_ids, model_ids, tag)

    def count_buffer_group_by_earlier(self, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_group_by_earlier(self, device_ids, model_ids, earlier, tag)

    def count_buffer_group_by_later(self, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_group_by_later(self, device_ids, model_ids, later, tag)

    def count_buffer_group_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None) -> int:
        return buffer.count_buffer_group_by_range(self, device_ids, model_ids, begin, end, tag)

    def read_slice(self, id: int) -> SliceSchema:
        return slice.read_slice(self, id)

    def list_slice_by_ids(self, ids: List[int]) -> List[SliceSchema]:
        return slice.list_slice_by_ids(self, ids)

    def list_slice_by_time(self, device_id: UUID, model_id: UUID, timestamp: datetime) -> List[SliceSchema]:
        return slice.list_slice_by_time(self, device_id, model_id, timestamp)

    def list_slice_by_range(self, device_id: UUID, model_id: UUID, begin: datetime, end: datetime) -> List[SliceSchema]:
        return slice.list_slice_by_range(self, device_id, model_id, begin, end)

    def list_slice_by_name_time(self, name: str, timestamp: datetime) -> List[SliceSchema]:
        return slice.list_slice_by_name_time(self, name, timestamp)

    def list_slice_by_name_range(self, name: str, begin: datetime, end: datetime) -> List[SliceSchema]:
        return slice.list_slice_by_name_range(self, name, begin, end)

    def list_slice_option(self, device_id: Optional[UUID], model_id: Optional[UUID], name: Optional[str], begin_or_timestamp: Optional[datetime], end: Optional[datetime]) -> List[SliceSchema]:
        return slice.list_slice_option(self, device_id, model_id, name, begin_or_timestamp, end)

    def list_slice_group_by_time(self, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime) -> List[SliceSchema]:
        return slice.list_slice_group_by_time(self, device_ids, model_ids, timestamp)

    def list_slice_group_by_range(self, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime) -> List[SliceSchema]:
        return slice.list_slice_group_by_range(self, device_ids, model_ids, begin, end)

    def list_slice_group_option(self, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], name: Optional[str], begin_or_timestamp: Optional[datetime], end: Optional[datetime]) -> List[SliceSchema]:
        return slice.list_slice_group_option(self, device_ids, model_ids, name, begin_or_timestamp, end)

    def create_slice(self, device_id: UUID, model_id: UUID, timestamp_begin: datetime, timestamp_end: datetime, name: str, description: str) -> int:
        return slice.create_slice(self, device_id, model_id, timestamp_begin, timestamp_end, name, description)

    def update_slice(self, id: int, timestamp_begin: Optional[datetime], timestamp_end: Optional[datetime], name: Optional[str], description: Optional[str]):
        return slice.update_slice(self, id, timestamp_begin, timestamp_end, name, description)

    def delete_slice(self, id: int):
        return slice.delete_slice(self, id)

    def read_slice_set(self, id: int) -> SliceSetSchema:
        return slice.read_slice_set(self, id)

    def list_slice_set_by_ids(self, ids: List[int]) -> List[SliceSetSchema]:
        return slice.list_slice_set_by_ids(self, ids)

    def list_slice_set_by_time(self, set_id: UUID, timestamp: datetime) -> List[SliceSetSchema]:
        return slice.list_slice_set_by_time(self, set_id, timestamp)

    def list_slice_set_by_range(self, set_id: UUID, begin: datetime, end: datetime) -> List[SliceSetSchema]:
        return slice.list_slice_set_by_range(self, set_id, begin, end)

    def list_slice_set_by_name_time(self, name: str, timestamp: datetime) -> List[SliceSetSchema]:
        return slice.list_slice_set_by_name_time(self, name, timestamp)

    def list_slice_set_by_name_range(self, name: str, begin: datetime, end: datetime) -> List[SliceSetSchema]:
        return slice.list_slice_set_by_name_range(self, name, begin, end)

    def list_slice_set_option(self, set_id: Optional[UUID], name: Optional[str], begin_or_timestamp: Optional[datetime], end: Optional[datetime]) -> List[SliceSetSchema]:
        return slice.list_slice_set_option(self, set_id, name, begin_or_timestamp, end)

    def create_slice_set(self, set_id: UUID, timestamp_begin: datetime, timestamp_end: datetime, name: str, description: str) -> int:
        return slice.create_slice_set(self, set_id, timestamp_begin, timestamp_end, name, description)

    def update_slice_set(self, id: int, timestamp_begin: Optional[datetime], timestamp_end: Optional[datetime], name: Optional[str], description: Optional[str]):
        return slice.update_slice_set(self, id, timestamp_begin, timestamp_end, name, description)

    def delete_slice_set(self, id: int):
        return slice.delete_slice_set(self, id)
