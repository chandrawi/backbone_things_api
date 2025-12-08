from ..proto.auth import auth_pb2, auth_pb2_grpc
from uuid import UUID
import grpc
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
from ._schema import AccessToken, UserLogin, UserRefresh


def user_login(auth, username: str, password: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = auth_pb2_grpc.AuthServiceStub(channel)
        request = auth_pb2.UserKeyRequest()
        response = stub.UserLoginKey(request)
        public_key = RSA.import_key(response.public_key)
        sha = SHA256.new()
        chipper_rsa = PKCS1_OAEP.new(public_key, sha)
        encrypted = chipper_rsa.encrypt(bytes(password, "utf-8"))
        request = auth_pb2.UserLoginRequest(
            username=username,
            password=encrypted
        )
        response = stub.UserLogin(request)
        access_tokens = []
        for token in response.access_tokens: 
            access_tokens.append(AccessToken(UUID(bytes=token.api_id), token.access_token, token.refresh_token))
        return UserLogin(UUID(bytes=response.user_id), response.auth_token, access_tokens)

def user_refresh(auth, api_id: UUID, access_token: str, refresh_token: str) -> UserRefresh:
    with grpc.insecure_channel(auth.address) as channel:
        stub = auth_pb2_grpc.AuthServiceStub(channel)
        request = auth_pb2.UserRefreshRequest(
            api_id=api_id.bytes,
            access_token=access_token,
            refresh_token=refresh_token
        )
        response = stub.UserRefresh(request)
        return UserRefresh(response.access_token, response.refresh_token)

def user_logout(auth, user_id: UUID, auth_token: str):
    with grpc.insecure_channel(auth.address) as channel:
        stub = auth_pb2_grpc.AuthServiceStub(channel)
        request = auth_pb2.UserLogoutRequest(
            user_id=user_id.bytes,
            auth_token=auth_token
        )
        stub.UserLogout(request)
