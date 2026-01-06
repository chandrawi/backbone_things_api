import { DataType, pack_type, pack_data, pack_data_array, unpack_data, unpack_data_array } from '../../bundle.js';

describe("Backbone Things type and value test", function() {

    it("should set value type from data", function() {
        expect(pack_type(-100)).toEqual(DataType.I32);
        expect(pack_type(1.23)).toEqual(DataType.F64);
        expect(pack_type(true)).toEqual(DataType.BOOL);
        expect(pack_type("a")).toEqual(DataType.CHAR);
        expect(pack_type("abc")).toEqual(DataType.STRING);
        expect(pack_type(null)).toEqual(DataType.NULL);
    });

    it("should set value binary from data", function() {
        expect(pack_data(-1000)).toEqual("///8GA==");
        expect(pack_data(1000)).toEqual("AAAD6A==");
        expect(pack_data(0.123)).toEqual("P7987ZFocrA=");
        expect(pack_data(true)).toEqual("AQ==");
        expect(pack_data(false)).toEqual("AA==");
        expect(pack_data("z")).toEqual("eg==");
        expect(pack_data("xyz")).toEqual("eHl6");
        expect(pack_data(null)).toEqual("");
    });

    it("should set value type and binary from list of data", function() {
        const values = [-1000, 0.123, false, "xyz", null];
        expect(pack_data_array(values)).toEqual("///8GD+/fO2RaHKwAAN4eXo=");
    });

    it("should get data from value type and binary", function() {
        expect(unpack_data("///8GA==", DataType.I32)).toEqual(-1000);
        expect(unpack_data("///8GA==", DataType.I16)).toEqual(-1000);
        expect(unpack_data("///8GA==", DataType.U16)).toEqual(64536);
        expect(unpack_data("P7987ZFocrA=", DataType.F64)).toEqual(0.123);
        expect(unpack_data("PfvnbQ==", DataType.F32)).toBeCloseTo(0.123, 0.000001);
        expect(unpack_data("RHoAAA==", DataType.F32)).toEqual(1000);
        expect(unpack_data("/wA=", DataType.BOOL)).toEqual(true);
        expect(unpack_data("AAAA", DataType.BOOL)).toEqual(false);
        expect(unpack_data("eg==", DataType.CHAR)).toEqual("z");
        expect(unpack_data("eHl6", DataType.CHAR)).toEqual("x");
        expect(unpack_data("eHl6", DataType.STRING)).toEqual("xyz");
        expect(unpack_data("", DataType.NULL)).toEqual(null);
    });

    it("should get data from list of value type and binary", function() {
        const data = unpack_data_array(
            "///8GD+/fO2RaHKwAAN4eXo=",
            [DataType.I32, DataType.F64, DataType.BOOL, DataType.STRING, DataType.NULL]
        );
        expect(data).toEqual([-1000, 0.123, false, "xyz", null]);
    });

});
