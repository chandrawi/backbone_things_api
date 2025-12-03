use bbthings_database::{DataValue, ArrayDataValue};
use bbthings_database::DataType::{I8T, I16T, I32T, I64T, U8T, U16T, U32T, U64T, F32T, F64T, BoolT, CharT, StringT, BytesT};
use bbthings_database::DataValue::{I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Bool, Char};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_value_conversion()
    {
        let value: i8 = -1;
        let data = DataValue::from(value);
        assert_eq!(value, i8::try_from(data).unwrap());
        let value: i16 = -256;
        let data = DataValue::from(value);
        assert_eq!(value, i16::try_from(data).unwrap());
        let value: i32 = -65536;
        let data = DataValue::from(value);
        assert_eq!(value, i32::try_from(data).unwrap());
        let value: i64 = -4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, i64::try_from(data).unwrap());

        let value: u8 = 1;
        let data = DataValue::from(value);
        assert_eq!(value, u8::try_from(data).unwrap());
        let value: u16 = 256;
        let data = DataValue::from(value);
        assert_eq!(value, u16::try_from(data).unwrap());
        let value: u32 = 65536;
        let data = DataValue::from(value);
        assert_eq!(value, u32::try_from(data).unwrap());
        let value: u64 = 4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, u64::try_from(data).unwrap());

        let value: f32 = 65536.65536;
        let data = DataValue::from(value);
        assert_eq!(value, f32::try_from(data).unwrap());
        let value: f64 = 4294967296.4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, f64::try_from(data).unwrap());

        let value: bool = true;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<bool>::try_into(data).unwrap());
        let value: char = 'a';
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<char>::try_into(data).unwrap());

        let value: String = "xyz".to_owned();
        let data = DataValue::from(value.clone());
        assert_eq!(value, TryInto::<String>::try_into(data).unwrap());
        let value: Vec<u8> = vec![101, 102, 103, 104, 105];
        let data = DataValue::from(value.clone());
        assert_eq!(value, TryInto::<Vec<u8>>::try_into(data).unwrap());
    }

    #[test]
    fn data_value_bytes() 
    {
        let bytes = [255];
        let value = DataValue::from_bytes(&bytes, I8T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I8(-1));
        let bytes = [255, 0];
        let value = DataValue::from_bytes(&bytes, I16T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I16(-256));
        let bytes = [255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, I32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I32(-256));
        let bytes = [255, 255, 255, 255, 255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, I64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I64(-256));

        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, U8T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U8(1));
        let bytes = [1, 0];
        let value = DataValue::from_bytes(&bytes, U16T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U16(256));
        let bytes = [1, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, U32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U32(16777216));
        let bytes = [1, 0, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, U64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U64(72057594037927936));

        let bytes = [62, 32, 0, 0];
        let value = DataValue::from_bytes(&bytes, F32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, F32(0.15625));
        let bytes = [63, 136, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, F64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, F64(0.01171875));

        let bytes = [97];
        let value = DataValue::from_bytes(&bytes, CharT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, Char('a'));
        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, BoolT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, Bool(true));

        let bytes = [97, 98, 99];
        let value = DataValue::from_bytes(&bytes, StringT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, DataValue::String("abc".to_owned()));
        let bytes = [10, 20, 30, 40];
        let value = DataValue::from_bytes(&bytes, BytesT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, DataValue::Bytes(vec![10, 20, 30, 40]));

        // wrong bytes length
        let bytes = [1, 0];
        assert_eq!(DataValue::from_bytes(&bytes, U8T), DataValue::Null);
    }

    #[test]
    fn array_data_value_bytes() 
    {
        let bytes = [1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let types = [U8T, I16T, U32T, I64T];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(bytes.to_vec(), data.to_bytes());
        assert_eq!(data.to_vec(), [
            U8(1),
            I16(256),
            U32(16777216),
            I64(72057594037927936)
        ]);

        let bytes = [62, 32, 0, 0, 63, 136, 0, 0, 0, 0, 0, 0];
        let types = [F32T, F64T];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(bytes.to_vec(), data.to_bytes());
        assert_eq!(data.to_vec(), [
            F32(0.15625),
            F64(0.01171875)
        ]);

        let bytes = [97, 1, 3, 97, 98, 99, 4, 10, 20, 30, 40];
        let types = [CharT, BoolT, StringT, BytesT];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(bytes.to_vec(), data.to_bytes());
        assert_eq!(data.to_vec(), [
            Char('a'),
            Bool(true),
            DataValue::String("abc".to_owned()),
            DataValue::Bytes(vec![10, 20, 30, 40])
        ]);
    }

}
