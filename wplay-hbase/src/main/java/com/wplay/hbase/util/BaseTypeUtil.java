package com.wplay.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 处理java中的基本类型如int,Integer,long,String,date等
 *
 * @author James
 *
 */
public class BaseTypeUtil {

    protected static final Map<Class<?>, Type> BASETYPEMAPING = new HashMap<Class<?>, Type>();
    static {
	BASETYPEMAPING.put(int.class, Type.INT);
	BASETYPEMAPING.put(float.class, Type.FLOAT);
	BASETYPEMAPING.put(long.class, Type.LONG);
	BASETYPEMAPING.put(byte.class, Type.BYTE);
	BASETYPEMAPING.put(double.class, Type.DOUBLE);
	BASETYPEMAPING.put(boolean.class, Type.BOOLEAN);
	BASETYPEMAPING.put(short.class, Type.SHORT);
	BASETYPEMAPING.put(char.class, Type.CHAR);

	BASETYPEMAPING.put(String.class, Type.STRING);
	BASETYPEMAPING.put(Date.class, Type.DATE);
	BASETYPEMAPING.put(Integer.class, Type.INT);
	BASETYPEMAPING.put(Float.class, Type.FLOAT);
	BASETYPEMAPING.put(Long.class, Type.LONG);
	BASETYPEMAPING.put(Byte.class, Type.BYTE);
	BASETYPEMAPING.put(Double.class, Type.DOUBLE);
	BASETYPEMAPING.put(Boolean.class, Type.BOOLEAN);
	BASETYPEMAPING.put(Short.class, Type.SHORT);
	BASETYPEMAPING.put(Character.class, Type.CHAR);

	BASETYPEMAPING.put(byte[].class, Type.BYTES);
    }

    /**
     * 是基本类型
     * 
     * @param clazz
     * @return
     */
    public static boolean isBaseType(Class<?> clazz) {
	return BASETYPEMAPING.keySet().contains(clazz);
    }

    public static Type getType(Class<?> clazz) {
	if (isBaseType(clazz)) {
	    return BASETYPEMAPING.get(clazz);
	}
	return Type.UNW;
    }

    private static Type getType(String className) {
	try {
	    return getType(Class.forName(className));
	} catch (ClassNotFoundException e) {
	    return Type.UNW;
	}
    }

    /**
     * 
     * @param className
     * @param value
     * @return
     */
    public static Object getValue(String className, byte[] value) {
	Type type = getType(className);
	return getValue(type, value);
    }

    /**
     * 
     * @param clazz
     * @param value
     * @return
     */
    public static Object getValue(Class<?> clazz, byte[] value) {
	return getValue(getType(clazz), value);
    }

    private static Object getValue(Type type, byte[] value) {
	try {
	    switch (type) {
	    case STRING:
		return Bytes.toString(value);
	    case INT:
		return Bytes.toInt(value);
	    case FLOAT:
		return Bytes.toFloat(value);
	    case LONG:
		return Bytes.toLong(value);
	    case DOUBLE:
		return Bytes.toDouble(value);
	    case SHORT:
		return Bytes.toShort(value);
	    case BOOLEAN:
		return Bytes.toBoolean(value);
	    case BYTE:
	    return value[0];
	    case UNW:
	    default:
		return null;
	    }
	} catch (Exception e) {
	    return null;
	}
    }
    
    /**
     * 
     * @param clazz
     * @param in
     * @return
     */
    public static Object getValue(Class<?> clazz, DataInput in) {
	return getValue(getType(clazz), in);
    }

    
    /**
     * 
     * @param type
     * @param in
     * @return
     */
    private static Object getValue(Type type, DataInput in) {
  	try {
  	    switch (type) {
  	    case STRING:
  		return WritableUtils.readString(in);
  	    case INT:
  		return WritableUtils.readVInt(in);
  	    case FLOAT:
  		return in.readFloat();
  	    case LONG:
  		return WritableUtils.readVLong(in);
  	    case DOUBLE:
  		return in.readDouble();
  	    case SHORT:
  		return in.readShort();
  	    case BOOLEAN:
  		return in.readBoolean();
  	    case BYTE:
  	    return in.readByte();
  	    case UNW:
  	    default:
  		return null;
  	    }
  	} catch (Exception e) {
  	    return null;
  	}
      }

    public static byte[] toBytes(Object value) throws IOException {
	Type type = getType(value.getClass());
	try {
	    switch (type) {
	    case STRING:
		return Bytes.toBytes((String) value);
	    case INT:
		return Bytes.toBytes((Integer) value);
	    case FLOAT:
		return Bytes.toBytes((Float) value);
	    case LONG:
		return Bytes.toBytes((Long) value);
	    case DOUBLE:
		return Bytes.toBytes((Double) value);
	    case SHORT:
		return Bytes.toBytes((Short) value);
	    case BOOLEAN:
		return Bytes.toBytes((Boolean) value);
	    case BYTES:
		return (byte[]) value;
	    case BYTE:
	    return new byte[]{(Byte)value}	;
	    case UNW:
	    default:
		return null;
	    }
	} catch (Exception e) {
	    throw new IOException(value.getClass() + " Unknow class");
	}
    }
}
