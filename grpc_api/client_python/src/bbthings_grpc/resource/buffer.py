from ..proto.resource import buffer_pb2, buffer_pb2_grpc
from typing import Optional, Union, List
from datetime import datetime
from uuid import UUID
import grpc
from ..common.type_value import DataType, pack_type, pack_data_array
from ..common.tag import Tag
from ._schema import BufferSchema, BufferSetSchema


def read_buffer(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferId(id=id)
        response = stub.ReadBuffer(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def read_buffer_by_time(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadBufferByTime(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def list_buffer_by_ids(resource, ids: List[int]):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferIds(
            ids=ids
        )
        response = stub.ListBufferByIds(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_time(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_number_before(resource, device_id: UUID, model_id: UUID, before: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferNumber(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(before.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListBufferByNumberBefore(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_by_number_after(resource, device_id: UUID, model_id: UUID, after: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferNumber(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(after.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListBufferByNumberAfter(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def read_buffer_first(resource, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BufferSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag
        )
        response = stub.ReadBufferFirst(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def read_buffer_last(resource, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BufferSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag
        )
        response = stub.ReadBufferLast(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def list_buffer_first(resource, number: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number
        )
        response = stub.ListBufferFirst(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_first_offset(resource, number: int, offset: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number,
            offset=offset
        )
        response = stub.ListBufferFirstOffset(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_last(resource, number: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number
        )
        response = stub.ListBufferLast(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_last_offset(resource, number: int, offset: int, device_id: Optional[UUID]=None, model_id: Optional[UUID]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number,
            offset=offset
        )
        response = stub.ListBufferLastOffset(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_time(resource, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_number_before(resource, device_ids: List[UUID], model_ids: List[UUID], before: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupNumber(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(before.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListBufferGroupByNumberBefore(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_by_number_after(resource, device_ids: List[UUID], model_ids: List[UUID], after: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupNumber(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(after.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListBufferGroupByNumberAfter(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def read_buffer_group_first(resource, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BufferGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            id_flag=id_flag
        )
        response = stub.ReadBufferGroupFirst(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def read_buffer_group_last(resource, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BufferGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            id_flag=id_flag
        )
        response = stub.ReadBufferGroupLast(request=request, metadata=resource.metadata)
        return BufferSchema.from_response(response.result)

def list_buffer_group_first(resource, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupFirst(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_first_offset(resource, number: int, offset: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            offset=offset,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupFirstOffset(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_last(resource, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupLast(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def list_buffer_group_last_offset(resource, number: int, offset: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            offset=offset,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupLastOffset(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSchema.from_response(result))
        return ls

def read_buffer_set(resource, set_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferSetTime(
            set_id=set_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadBufferSet(request=request, metadata=resource.metadata)
        return BufferSetSchema.from_response(response.result)

def list_buffer_set_by_time(resource, set_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferSetTime(
            set_id=set_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferSetByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSetSchema.from_response(result))
        return ls

def list_buffer_set_by_earlier(resource, set_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferSetEarlier(
            set_id=set_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferSetByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSetSchema.from_response(result))
        return ls

def list_buffer_set_by_later(resource, set_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferSetLater(
            set_id=set_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferSetByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSetSchema.from_response(result))
        return ls

def list_buffer_set_by_range(resource, set_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferSetRange(
            set_id=set_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferSetByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(BufferSetSchema.from_response(result))
        return ls

def create_buffer(resource, device_id: UUID, model_id: UUID, timestamp: datetime, data: List[Union[int, float, str, bool, None]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        data_type = []
        for d in data: data_type.append(pack_type(d))
        request = buffer_pb2.BufferSchema(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            data_bytes=pack_data_array(data),
            data_type=data_type,
            tag=tag
        )
        response = stub.CreateBuffer(request=request, metadata=resource.metadata)
        return response.id

def create_buffer_multiple(resource, device_ids: list[UUID], model_ids: list[UUID], timestamps: list[datetime], data: List[List[Union[int, float, str, bool, None]]], tags: Optional[List[int]]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        number = len(device_ids)
        if tags is None: tags = [Tag.DEFAULT] * number
        if any(length != number for length in [len(model_ids), len(timestamps), len(data), len(tags)]):
            raise grpc.RpcError(grpc.StatusCode.INVALID_ARGUMENT)
        schemas = []
        for i in range(number):
            data_type = []
            for d in data[i]: data_type.append(pack_type(d))
            schemas.append(buffer_pb2.BufferSchema(
                device_id=device_ids[i].bytes,
                model_id=model_ids[i].bytes,
                timestamp=int(timestamps[i].timestamp()*1000000),
                data_bytes=pack_data_array(data[i]),
                data_type=data_type,
                tag=tags[i]
            ))
        request = buffer_pb2.BufferMultipleSchema(schemas=schemas)
        response = stub.CreateBufferMultiple(request=request, metadata=resource.metadata)
        return response.ids

def update_buffer(resource, id: int, data: Optional[List[Union[int, float, str, bool, None]]]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        data_bytes = None
        data_list = []
        if data != None: 
            data_bytes = pack_data_array(data)
            data_list = data
        data_type = []
        for d in data_list: data_type.append(pack_type(d))
        request = buffer_pb2.BufferUpdate(
            id=id,
            data_bytes=data_bytes,
            data_type=data_type,
            tag=tag
        )
        stub.UpdateBuffer(request=request, metadata=resource.metadata)

def update_buffer_by_time(resource, device_id: UUID, model_id: UUID, timestamp: datetime, data: Optional[List[Union[int, float, str, bool, None]]]=None, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        data_bytes = None
        data_list = []
        if data != None: 
            data_bytes = pack_data_array(data)
            data_list = data
        data_type = []
        for d in data_list: data_type.append(pack_type(d))
        request = buffer_pb2.BufferUpdateTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            data_bytes=data_bytes,
            data_type=data_type,
            tag=tag
        )
        stub.UpdateBufferByTime(request=request, metadata=resource.metadata)

def delete_buffer(resource, id: int):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferId(id=id)
        stub.DeleteBuffer(request=request, metadata=resource.metadata)

def delete_buffer_by_time(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        stub.DeleteBufferByTime(request=request, metadata=resource.metadata)

def read_buffer_timestamp(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadBufferTimestamp(request=request, metadata=resource.metadata)
        return datetime.fromtimestamp(response.timestamp/1000000.0)

def list_buffer_timestamp_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferTimestampByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_timestamp_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferTimestampByLater(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_timestamp_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferTimestampByRange(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_timestamp_first(resource, number: int, device_id: Optional[UUID], model_id: Optional[UUID], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number
        )
        response = stub.ListBufferTimestampFirst(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_timestamp_last(resource, number: int, device_id: Optional[UUID], model_id: Optional[UUID], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        device_bytes = None
        if device_id != None: device_bytes = device_id.bytes
        model_bytes = None
        if model_id != None: model_bytes = model_id.bytes
        request = buffer_pb2.BuffersSelector(
            device_id=device_bytes,
            model_id=model_bytes,
            tag=tag,
            number=number
        )
        response = stub.ListBufferTimestampLast(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def read_buffer_group_timestamp(resource, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadBufferGroupTimestamp(request=request, metadata=resource.metadata)
        return datetime.fromtimestamp(response.timestamp/1000000.0)

def list_buffer_group_timestamp_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupTimestampByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_group_timestamp_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupTimestampByLater(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_group_timestamp_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListBufferGroupTimestampByRange(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_group_timestamp_first(resource, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupTimestampFirst(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_buffer_group_timestamp_last(resource, number: int, device_ids: Optional[List[UUID]], model_ids: Optional[List[UUID]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        id_flag = 0
        device_bytes_list = None
        if device_ids != None:
            id_flag += 1
            device_bytes_list = list(map((lambda x: x.bytes), device_ids))
        model_bytes_list = None
        if model_ids != None:
            id_flag += 2
            model_bytes_list = list(map((lambda x: x.bytes), model_ids))
        request = buffer_pb2.BuffersGroupSelector(
            device_ids=device_bytes_list,
            model_ids=model_bytes_list,
            tag=tag,
            number=number,
            id_flag=id_flag
        )
        response = stub.ListBufferGroupTimestampLast(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def count_buffer(resource, device_id: UUID, model_id: UUID, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            tag=tag
        )
        response = stub.CountBuffer(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferByEarlier(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferByLater(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferByRange(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_group(resource, device_ids: List[UUID], model_ids: List[UUID], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            tag=tag
        )
        response = stub.CountBufferGroup(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_group_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferGroupByEarlier(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_group_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferGroupByLater(request=request, metadata=resource.metadata)
        return response.count

def count_buffer_group_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = buffer_pb2_grpc.BufferServiceStub(channel)
        request = buffer_pb2.BufferGroupRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountBufferGroupByRange(request=request, metadata=resource.metadata)
        return response.count
