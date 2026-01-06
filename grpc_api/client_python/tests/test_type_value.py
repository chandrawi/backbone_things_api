import os
import sys

SOURCE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),"src")
sys.path.append(SOURCE_PATH)

import pytest
import math
from bbthings_grpc.common.type_value import (
    DataType, 
    pack_type, pack_data, pack_data_array, pack_data_type, 
    unpack_data, unpack_data_array
)


def test_type_value():

    assert pack_type(-100) == DataType.I64.value
    assert pack_type(1.23) == DataType.F64.value
    assert pack_type(True) == DataType.BOOL.value
    assert pack_type("a") == DataType.CHAR.value
    assert pack_type("abc") == DataType.STRING.value
    assert pack_type(None) == DataType.NULL.value

    assert pack_data(-1000) == b'\xff\xff\xff\xff\xff\xff\xfc\x18'
    assert pack_data(1000) == b'\x00\x00\x00\x00\x00\x00\x03\xe8'
    assert pack_data(0.123) == b'\x3f\xbf\x7c\xed\x91\x68\x72\xb0'
    assert pack_data(True) == b'\x01'
    assert pack_data(False) == b'\x00'
    assert pack_data("z") == b'\x7a'
    assert pack_data("xyz") == b'\x78\x79\x7a'
    assert pack_data(b'\x00\xff') == b'\x00\xff'
    assert pack_data(None) == b''

    assert pack_data_array(
        [-1000, 0.123, False, "xyz", None]
    ) == b'\xff\xff\xff\xff\xff\xff\xfc\x18\x3f\xbf\x7c\xed\x91\x68\x72\xb0\x00\x03\x78\x79\x7a'

    assert pack_data_type(-1000, DataType.I64) == b'\xff\xff\xff\xff\xff\xff\xfc\x18'
    assert pack_data_type(-1000, DataType.I32) == b'\xff\xff\xfc\x18'
    with pytest.raises(Exception):
        assert pack_data_type(-1000, DataType.U16) == b'\xff\xff\xfc\x18'
    assert pack_data_type(0.123, DataType.F64) == b'\x3f\xbf\x7c\xed\x91\x68\x72\xb0'
    assert pack_data_type(0.123, DataType.F32) == b'\x3d\xfb\xe7\x6d'
    assert pack_data_type(1000, DataType.F32) == b'\x44\x7a\x00\x00'
    assert pack_data_type(True, DataType.BOOL) == b'\x01'
    assert pack_data_type(0, DataType.BOOL) == b'\x00'
    assert pack_data_type(100, DataType.BOOL) == b'\x01'
    assert pack_data_type("z", DataType.CHAR) == b'\x7a'
    assert pack_data_type("xyz", DataType.CHAR) == b'\x78'
    assert pack_data_type("xyz", DataType.STRING) == b'\x78\x79\x7A'
    assert pack_data_type(None, DataType.NULL) == b''

    assert unpack_data(b'\xff\xff\xff\xff\xff\xff\xfc\x18', DataType.I64) == -1000
    assert unpack_data(b'\xff\xff\xfc\x18', DataType.I32) == -1000
    assert unpack_data(b'\xff\xff\xfc\x18', DataType.I16) == -1000
    assert unpack_data(b'\xff\xff\xfc\x18', DataType.U16) == 64536
    assert unpack_data(b'\x3f\xbf\x7c\xed\x91\x68\x72\xb0', DataType.F64) == 0.123
    assert math.isclose(unpack_data(b'\x3d\xfb\xe7\x6d', DataType.F32), 0.123, abs_tol=0.000001)
    assert unpack_data(b'\x44\x7a\x00\x00', DataType.F32) == 1000
    assert unpack_data(b'\xff\x00', DataType.BOOL) == True
    assert unpack_data(b'\x00\x00\x00', DataType.BOOL) == False
    assert unpack_data(b'\x7a', DataType.CHAR) == 'z'
    assert unpack_data(b'\x78\x79\x7A', DataType.CHAR) == 'x'
    assert unpack_data(b'\x78\x79\x7A', DataType.STRING) == 'xyz'
    assert unpack_data(b'\x00\xff', DataType.NULL) == None

    assert unpack_data_array(
        b'\xff\xff\xff\xff\xff\xff\xfc\x18\x3f\xbf\x7c\xed\x91\x68\x72\xb0\x00\x03\x78\x79\x7a', 
        [DataType.I64, DataType.F64, DataType.BOOL, DataType.STRING, DataType.NULL]
    ) == [-1000, 0.123, False, "xyz", None]
