package com.wplay.hbase.util;

import org.apache.hadoop.io.WritableUtils;

import java.io.DataOutput;
import java.io.IOException;

public class BaseTypeWrite extends BaseTypeUtil {

    public static void write(DataOutput out, Object value) throws IOException {
	Type type = getType(value.getClass());
	switch (type) {
	case STRING:
	    WritableUtils.writeString(out, (String) value);
	    break;
	case INT:
	    out.writeInt((Integer) value);
	    break;
	case FLOAT:
	    out.writeFloat((Float) value);
	    break;
	case LONG:
	    out.writeLong((Long) value);
	    break;
	case DOUBLE:
	    out.writeDouble((Double) value);
	    break;
	case SHORT:
	    out.writeShort((Short) value);
	    break;
	case BOOLEAN:
	    out.writeBoolean((Boolean) value);
	    break;
	default:
	    break;
	}
    }
}
