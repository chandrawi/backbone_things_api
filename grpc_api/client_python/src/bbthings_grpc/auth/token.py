from ..proto.auth import token_pb2, token_pb2_grpc
from typing import Optional
from datetime import datetime
from uuid import UUID
import grpc
from ._schema import TokenSchema


def read_access_token(auth, access_id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.AccessId(access_id=access_id)
        response = stub.ReadAccessToken(request=request, metadata=auth.metadata)
        return TokenSchema.from_response(response.result)

def list_auth_token(auth, auth_token: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.AuthToken(auth_token=auth_token)
        response = stub.ListAuthToken(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_user(auth, user_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.UserId(user_id=user_id.bytes)
        response = stub.ListTokenByUser(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_created_earlier(auth, earlier: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenTime(
            timestamp=int(earlier.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByCreatedEarlier(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_created_later(auth, later: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenTime(
            timestamp=int(later.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByCreatedLater(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_created_range(auth, begin: datetime, end: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenRangeSingle(
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByCreatedRange(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_expired_earlier(auth, earlier: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenTime(
            timestamp=int(earlier.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByExpiredEarlier(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_expired_later(auth, later: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenTime(
            timestamp=int(later.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByExpiredLater(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_expired_range(auth, begin: datetime, end: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenRangeSingle(
            begin=int(begin.timestamp()*1000000),
            end=int(end.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByExpiredRange(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def list_token_by_range(auth, b_created: datetime, e_created: datetime, b_expired: datetime, e_expired: datetime, user_id: Optional[UUID]=None):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        user_id_bytes = None if user_id == None else user_id.bytes
        request = token_pb2.TokenRangeDouble(
            begin_1=int(b_created.timestamp()*1000000),
            end_1=int(e_created.timestamp()*1000000),
            begin_2=int(b_expired.timestamp()*1000000),
            end_2=int(e_expired.timestamp()*1000000),
            user_id=user_id_bytes
        )
        response = stub.ListTokenByRange(request=request, metadata=auth.metadata)
        ls = []
        for result in response.results: ls.append(TokenSchema.from_response(result))
        return ls

def create_access_token(auth, user_id: UUID, auth_token: str, expired: datetime, ip: bytes):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.TokenSchema(
            user_id=user_id.bytes,
            auth_token=auth_token,
            expired=int(expired.timestamp()*1000000),
            ip=ip
        )
        response = stub.CreateAccessToken(request=request, metadata=auth.metadata)
        return (response.access_id, response.refresh_token, response.auth_token)

def create_auth_token(auth, user_id: UUID, expired: datetime, ip: bytes, number: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.AuthTokenCreate(
            user_id=user_id.bytes,
            number=number,
            expired=int(expired.timestamp()*1000000),
            ip=ip
        )
        response = stub.CreateAuthToken(request=request, metadata=auth.metadata)
        tokens = []
        for token in response.tokens: tokens.append((token.access_id, token.refresh_token, token.auth_token))
        return tokens

def update_access_token(auth, access_id: int, expired: Optional[datetime], ip: Optional[bytes]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.TokenUpdate(
            access_id=access_id,
            expired=int(expired.timestamp()*1000000),
            ip=ip
        )
        response = stub.UpdateAccessToken(request=request, metadata=auth.metadata)
        return response.refresh_token

def update_auth_token(auth, auth_token: str, expired: Optional[datetime], ip: Optional[bytes]):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.TokenUpdate(
            auth_token=auth_token,
            expired=int(expired.timestamp()*1000000),
            ip=ip
        )
        response = stub.UpdateAuthToken(request=request, metadata=auth.metadata)
        return response.refresh_token

def delete_access_token(auth, access_id: int):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.AccessId(access_id=access_id)
        stub.DeleteAccessToken(request=request, metadata=auth.metadata)

def delete_auth_token(auth, auth_token: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.AuthToken(auth_token=auth_token)
        stub.DeleteAuthToken(request=request, metadata=auth.metadata)

def delete_token_by_user(auth, user_id: UUID):
    with grpc.insecure_channel(auth.address) as channel:
        stub = token_pb2_grpc.TokenServiceStub(channel)
        request = token_pb2.UserId(user_id=user_id.bytes)
        stub.DeleteTokenByUser(request=request, metadata=auth.metadata)
