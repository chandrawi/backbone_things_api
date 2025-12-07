from bbthings_grpc_proto.resource import data_pb2, data_pb2_grpc
from typing import Optional, Union, List
from datetime import datetime
from uuid import UUID
import grpc
from ..common.type_value import DataType, pack_data_array
from ..common.tag import Tag
from ._schema import DataSchema, DataSetSchema


def read_data(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadData(request=request, metadata=resource.metadata)
        return DataSchema.from_response(response.result)

def list_data_by_time(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_by_number_before(resource, device_id: UUID, model_id: UUID, before: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataNumber(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(before.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListDataByNumberBefore(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_by_number_after(resource, device_id: UUID, model_id: UUID, after: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataNumber(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(after.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListDataByNumberAfter(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_time(resource, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_number_before(resource, device_ids: List[UUID], model_ids: List[UUID], before: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupNumber(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(before.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListDataGroupByNumberBefore(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def list_data_group_by_number_after(resource, device_ids: List[UUID], model_ids: List[UUID], after: datetime, number: int, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupNumber(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(after.timestamp()*1000000),
            number=number,
            tag=tag
        )
        response = stub.ListDataGroupByNumberAfter(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSchema.from_response(result))
        return ls

def read_data_set(resource, set_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataSetTime(
            set_id=set_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadDataSet(request=request, metadata=resource.metadata)
        return DataSetSchema.from_response(response.result)

def list_data_set_by_time(resource, set_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataSetTime(
            set_id=set_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataSetByTime(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSetSchema.from_response(result))
        return ls

def list_data_set_by_earlier(resource, set_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataSetEarlier(
            set_id=set_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataSetByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSetSchema.from_response(result))
        return ls

def list_data_set_by_later(resource, set_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataSetLater(
            set_id=set_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataSetByLater(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSetSchema.from_response(result))
        return ls

def list_data_set_by_range(resource, set_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataSetRange(
            set_id=set_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataSetByRange(request=request, metadata=resource.metadata)
        ls = []
        for result in response.results: ls.append(DataSetSchema.from_response(result))
        return ls

def create_data(resource, device_id: UUID, model_id: UUID, timestamp: datetime, data: List[Union[int, float, str, bool, None]], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        data_type = []
        for d in data: data_type.append(DataType.from_value(d).value)
        request = data_pb2.DataSchema(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            data_bytes=pack_data_array(data),
            data_type=data_type,
            tag=tag if tag is not None else 0
        )
        stub.CreateData(request=request, metadata=resource.metadata)

def create_data_multiple(resource, device_ids: List[UUID], model_ids: List[UUID], timestamps: List[datetime], data: List[List[Union[int, float, str, bool, None]]], tags: Optional[List[int]]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        number = len(device_ids)
        if tags is None: tags = [Tag.DEFAULT] * number
        if any(length != number for length in [len(model_ids), len(timestamps), len(data), len(tags)]):
            raise grpc.RpcError(grpc.StatusCode.INVALID_ARGUMENT)
        schemas = []
        for i in range(number):
            data_type = []
            for d in data[i]: data_type.append(DataType.from_value(d).value)
            schemas.append(data_pb2.DataSchema(
                device_id=device_ids[i].bytes,
                model_id=model_ids[i].bytes,
                timestamp=int(timestamps[i].timestamp()*1000000),
                data_bytes=pack_data_array(data[i]),
                data_type=data_type,
                tag=tags[i]
            ))
        request = data_pb2.DataMultipleSchema(schemas=schemas)
        stub.CreateDataMultiple(request=request, metadata=resource.metadata)

def delete_data(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        stub.DeleteData(request=request, metadata=resource.metadata)

def read_data_timestamp(resource, device_id: UUID, model_id: UUID, timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadDataTimestamp(request=request, metadata=resource.metadata)
        return datetime.fromtimestamp(response.timestamp/1000000.0)

def list_data_timestamp_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataTimestampByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_data_timestamp_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataTimestampByLater(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_data_timestamp_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataTimestampByRange(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def read_data_group_timestamp(resource, device_ids: List[UUID], model_ids: List[UUID], timestamp: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            timestamp=int(timestamp.timestamp()*1000000),
            tag=tag
        )
        response = stub.ReadDataGroupTimestamp(request=request, metadata=resource.metadata)
        return datetime.fromtimestamp(response.timestamp/1000000.0)

def list_data_group_timestamp_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupTimestampByEarlier(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_data_group_timestamp_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupTimestampByLater(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def list_data_group_timestamp_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.ListDataGroupTimestampByRange(request=request, metadata=resource.metadata)
        ls = []
        for timestamp in response.timestamps: ls.append(datetime.fromtimestamp(timestamp/1000000.0))
        return ls

def count_data(resource, device_id: UUID, model_id: UUID, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataTime(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            tag=tag
        )
        response = stub.CountData(request=request, metadata=resource.metadata)
        return response.count

def count_data_by_earlier(resource, device_id: UUID, model_id: UUID, earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataEarlier(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataByEarlier(request=request, metadata=resource.metadata)
        return response.count

def count_data_by_later(resource, device_id: UUID, model_id: UUID, later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataLater(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataByLater(request=request, metadata=resource.metadata)
        return response.count

def count_data_by_range(resource, device_id: UUID, model_id: UUID, begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataRange(
            device_id=device_id.bytes,
            model_id=model_id.bytes,
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataByRange(request=request, metadata=resource.metadata)
        return response.count

def count_data_group(resource, device_ids: List[UUID], model_ids: List[UUID], tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupTime(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            tag=tag
        )
        response = stub.CountDataGroup(request=request, metadata=resource.metadata)
        return response.count

def count_data_group_by_earlier(resource, device_ids: List[UUID], model_ids: List[UUID], earlier: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupEarlier(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            earlier=int(earlier.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataGroupByEarlier(request=request, metadata=resource.metadata)
        return response.count

def count_data_group_by_later(resource, device_ids: List[UUID], model_ids: List[UUID], later: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupLater(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            later=int(later.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataGroupByLater(request=request, metadata=resource.metadata)
        return response.count

def count_data_group_by_range(resource, device_ids: List[UUID], model_ids: List[UUID], begin: datetime, end: datetime, tag: Optional[int]=None):
    with grpc.insecure_channel(resource.address) as channel:
        stub = data_pb2_grpc.DataServiceStub(channel)
        request = data_pb2.DataGroupRange(
            device_ids=list(map((lambda x: x.bytes), device_ids)),
            model_ids=list(map((lambda x: x.bytes), model_ids)),
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            tag=tag
        )
        response = stub.CountDataGroupByRange(request=request, metadata=resource.metadata)
        return response.count
