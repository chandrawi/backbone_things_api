/**
 * @enum {number}
 */
export const DataType = Object.freeze({
    NULL: 0,
    I8: 1,
    I16: 2,
    I32: 3,
    I64: 4,
    I128: 5,
    U8: 6,
    U16: 7,
    U32: 8,
    U64: 9,
    U128: 10,
    F32: 12,
    F64: 13,
    BOOL: 15,
    CHAR: 16,
    STRING: 17,
    BYTES: 18
});

/**
 * @param {number|string} type
 * @returns {number}
 */
export function set_data_type(type) {
    if (typeof type === "number") {
        if (type >= 0 && type <= 18) {
            return type;
        }
    }
    else if (typeof type === "string") {
        switch (type.toUpperCase()) {
            case "I8": return DataType.I8;
            case "I16": return DataType.I16;
            case "I32": return DataType.I32;
            case "I64": return DataType.I64;
            case "I128": return DataType.I128;
            case "U8": return DataType.U8;
            case "U16": return DataType.U16;
            case "U32": return DataType.U32;
            case "U64": return DataType.U64;
            case "U128": return DataType.U128;
            case "F32": return DataType.F32;
            case "F64": return DataType.F64;
            case "BOOL": return DataType.BOOL;
            case "CHAR": return DataType.CHAR;
            case "STRING": return DataType.STRING;
            case "BYTES": return DataType.BYTES;
        }
    }
    return DataType.NULL;
}

/**
 * @param {number} type 
 * @returns {string}
 */
export function get_data_type(type) {
    switch (type) {
        case DataType.I8: return "I8";
        case DataType.I16: return "I16";
        case DataType.I32: return "I32";
        case DataType.I64: return "I64";
        case DataType.I128: return "I128";
        case DataType.U8: return "U8";
        case DataType.U16: return "U16";
        case DataType.U32: return "U32";
        case DataType.U64: return "U64";
        case DataType.U128: return "U128";
        case DataType.F32: return "F32";
        case DataType.F64: return "F64";
        case DataType.BOOL: return "BOOL";
        case DataType.CHAR: return "CHAR";
        case DataType.STRING: return "STRING";
        case DataType.BYTES: return "BYTES";
    }
    return "NULL";
}

/**
 * @param {string} base64 
 * @returns {ArrayBufferLike}
 */
function base64_to_array_buffer(base64) {
    let binaryString = atob(base64);
    let bytes = new Uint8Array(binaryString.length);
    for (let i=0; i<binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes.buffer;
}

/**
 * @param {ArrayBufferLike} buffer 
 * @returns {string}
 */
function array_buffer_to_base64(buffer) {
    let bytes = new Uint8Array(buffer);
    let binaryString = String.fromCharCode.apply(null, bytes);
    return btoa(binaryString);
}

/**
 * @param {string|ArrayBufferLike} base64 
 * @param {number} type 
 * @returns {number|bigint|string|Uint8Array|boolean|null}
 */
export function unpack_data(base64, type) {
    const buffer = base64_to_array_buffer(base64);
    const array = new Uint8Array(buffer);
    const view = new DataView(buffer);
    switch (type) {
        case DataType.I8: 
            if (view.byteLength >= 1) return view.getInt8(view.byteLength - 1);
        case DataType.I16: 
            if (view.byteLength >= 2) return view.getInt16(view.byteLength - 2);
        case DataType.I32: 
            if (view.byteLength >= 4) return view.getInt32(view.byteLength - 4);
        case DataType.I64: 
            if (view.byteLength >= 8) return view.getBigInt64(view.byteLength - 8);
        case DataType.I128: 
            if (view.byteLength >= 8) return view.getBigInt64(view.byteLength - 8);
        case DataType.U8: 
            if (view.byteLength >= 1) return view.getUint8(view.byteLength - 1);
        case DataType.U16: 
            if (view.byteLength >= 2) return view.getUint16(view.byteLength - 2);
        case DataType.U32: 
            if (view.byteLength >= 4) return view.getUint32(view.byteLength - 4);
        case DataType.U64: 
            if (view.byteLength >= 8) return view.getBigUint64(view.byteLength - 8);
        case DataType.U128: 
            if (view.byteLength >= 8) return view.getBigUint64(view.byteLength - 8);
        case DataType.F32:
            if (view.byteLength >= 4) return view.getFloat32(view.byteLength - 4);
        case DataType.F64:
            if (view.byteLength >= 8) return view.getFloat64(view.byteLength - 8);
        case DataType.BOOL:
            for (const byte of array) if (byte) return true;
            return false;
        case DataType.CHAR:
            if (view.byteLength >= 1) return String.fromCharCode(view.getUint8());
        case DataType.STRING:
            return new TextDecoder("utf-8").decode(array);
        case DataType.BYTES:
            return array;
    }
    return null;
}

/**
 * @param {string} base64 
 * @param {number[]} types 
 * @returns {(number|bigint|string|Uint8Array|boolean|null)[]}
 */
export function unpack_data_array(base64, types) {
    const buffer = base64_to_array_buffer(base64);
    let index = 0;
    let values = [];
    for (const type of types) {
        let length = 0;
        if (type == DataType.I8 || type == DataType.U8 || type == DataType.CHAR || type == DataType.BOOL) {
            length = 1;
        }
        else if (type == DataType.I16 || type == DataType.U16) {
            length = 2;
        }
        else if (type == DataType.I32 || type == DataType.U32 || type == DataType.F32) {
            length = 4;
        }
        else if (type == DataType.I64 || type == DataType.U64 || type == DataType.F64) {
            length = 8;
        }
        else if (type == DataType.I128 || type == DataType.U128) {
            length = 16;
        }
        else if (type == DataType.STRING || type == DataType.BYTES) {
            length = 1;
            if (index < buffer.byteLength) {
                const view = new DataView(buffer.slice(index));
                length = view.getUint8();
                index += 1;
            }
        }
        if (index + length > buffer.byteLength) break;
        const value = unpack_data(array_buffer_to_base64(buffer.slice(index, index + length)), type);
        values.push(value);
        index += length;
    }
    return values;
}

/**
 * @param {number|bigint|string|Uint8Array|boolean} value
 */
export function pack_type(value) {
    if (typeof value == "number") {
        if (Number.isInteger(value)) {
            return DataType.I32;
        }
        else {
            return DataType.F64;
        }
    }
    else if (typeof value == "bigint") {
        return DataType.I64;
    }
    else if (typeof value == "string") {
        if (value.length == 1) {
            return DataType.CHAR;
        }
        else {
            return DataType.STRING;
        }
    }
    else if (value instanceof Uint8Array) {
        return DataType.BYTES;
    }
    else if (typeof value == "boolean") {
        return DataType.BOOL;
    }
    return DataType.NULL;
}

/**
 * @param {number|bigint|string|Uint8Array|boolean} value
 */
function pack(value) {
    if (typeof value == "number") {
        if (Number.isInteger(value)) {
            const buffer = new ArrayBuffer(4);
            const view = new DataView(buffer);
            view.setInt32(0, value);
            return view.buffer;
        } else {
            const buffer = new ArrayBuffer(8);
            const view = new DataView(buffer);
            view.setFloat64(0, value);
            return view.buffer;
        }
    }
    else if (typeof value == "bigint") {
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        view.setBigInt64(0, value);
        return view.buffer;
    }
    else if (typeof value == "string") {
        const array = new Uint8Array(value.length);
        array.set(new TextEncoder("utf-8").encode(value));
        return array.buffer;
    }
    else if (value instanceof Uint8Array) {
        return value.buffer;
    }
    else if (typeof value == "boolean") {
        let array = new Uint8Array([0]);
        if (value) array = new Uint8Array([1]);
        return array.buffer;
    }
    return new ArrayBuffer(0);
}

/**
 * @param {number|bigint|string|Uint8Array|boolean} value
 */
export function pack_data(value) {
    return array_buffer_to_base64(pack(value));
}

/**
 * @param {(number|bigint|string|Uint8Array|boolean)[]} values
 */
export function pack_data_array(values) {
    if (values === undefined) {
        return "";
    }
    let arrays = new Uint8Array();
    for (const value of values) {
        let data_buffer = pack(value);
        if ((typeof value == "string" && value.length != 1) || value instanceof Uint8Array) {
            const len = new Uint8Array([value.length % 256]);
            const combine = new Uint8Array(arrays.byteLength + 1);
            combine.set(arrays);
            combine.set(len, arrays.byteLength);
            arrays = combine;
        }
        let array = new Uint8Array(data_buffer);
        let combine = new Uint8Array(arrays.byteLength + array.byteLength);
        combine.set(arrays);
        combine.set(array, arrays.byteLength);
        arrays = combine;
    }
    return array_buffer_to_base64(arrays.buffer);
}
