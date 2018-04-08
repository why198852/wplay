package com.wplay.hbase.util;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;

/**
 * 
 * @author James
 * 
 */
public class WriteableUtil {
    
    /**
     * 
     * @param array
     * @param componentType
     * @return
     * @throws IOException
     */
    public static final byte[] writeArray(Object array,Class<?> componentType) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dataOut = new DataOutputStream(out);
	WritableUtils.writeString(dataOut, componentType.getName());//Array 写类名
	int len = Array.getLength(array);
	WritableUtils.writeVInt(dataOut, len); //Collection 的数据size
	if (BaseTypeUtil.isBaseType(componentType)) {
	    for(int i = 0;i < len;i ++){
		dataOut.write(BaseTypeUtil.toBytes(Array.get(array, i)));
	    }
	}else if (Writable.class.isAssignableFrom(componentType)) {
	    for(int i = 0;i < len;i ++){
		Object obj = Array.get(array, i);
		writeObject((Writable) obj,dataOut);
	    }
	} else if (Enum.class.isAssignableFrom(componentType)) {
	    for(int i = 0;i < len;i ++){
		Object obj = Array.get(array, i);
		writeEnum((Enum<?>) obj, dataOut);
	    }
	}else {
	    throw new IOException("Type : \"" + componentType + "\" is unsupport");
	}
	dataOut.close();
	return out.toByteArray();
    }

    /**
     *
     * @param value
     * @param componentType
     * @return
     * @throws IOException
     */
    public static final Object readArray(byte[] value,Class<?> componentType) throws Exception {
	DataInputStream in = new DataInputStream(new ByteArrayInputStream(value));
	String className = WritableUtils.readString(in);
	int len = WritableUtils.readVInt(in);
	Object array = Array.newInstance(componentType, len);
	if (BaseTypeUtil.isBaseType(componentType)) {
	    for(int i = 0;i < len;i ++){
		Object child = BaseTypeUtil.getValue(componentType, in);
		Array.set(array, i, child);
	    }
	} else if (Writable.class.isAssignableFrom(componentType)) {
	    for(int i = 0;i < len;i ++){
		Object child = readObject(in);
		Array.set(array, i, child);
	    }
	} else if (Enum.class.isAssignableFrom(componentType)) {
	    for(int i = 0;i < len;i ++){
		Array.set(array, i, readEnum(in));
	    }
	}else {
	    throw new IOException("Type : \"" + componentType + "\" is unsupport");
	}
	in.close();
	return array;
    }

    /**
     * 把集合转成一个byte数组
     * @param coll
     * @param ownerType 集合包含的类型
     * @return
     * @throws IOException
     */
    public static final byte[] writeCollection(Collection<?> coll,Class<?> ownerType) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dataOut = new DataOutputStream(out);
	WritableUtils.writeString(dataOut, coll.getClass().getName());//Cllection 写类名
	WritableUtils.writeVInt(dataOut, coll.size()); //Collection 的数据size
	Iterator<?> iter = coll.iterator();
	if (BaseTypeUtil.isBaseType(ownerType)) {
	    while (iter.hasNext()) {
		Object o = iter.next();
		dataOut.write(BaseTypeUtil.toBytes(o));
	    }
	} else if (Writable.class.isAssignableFrom(ownerType)) {
	    while (iter.hasNext()) {
		Object o = iter.next();
		writeObject((Writable) o,dataOut);
	    }
	} else if (Enum.class.isAssignableFrom(ownerType)) {
	    while (iter.hasNext()) {
		Object o = iter.next();
		writeEnum((Enum<?>) o, dataOut);
	    }
	}else {
	    throw new IOException("Type : \"" + ownerType + "\" is unsupport");
	}
	dataOut.close();
	return out.toByteArray();
    }
    
    /**
     * 
     * @param value
     * @return
     * @throws IOException
     */
    public static final Collection<?> readCollection(byte[] value,Class<?> ownerType) throws Exception {
	DataInputStream in = new DataInputStream(new ByteArrayInputStream(value));
	String className = WritableUtils.readString(in);
	Collection<Object> coll = (Collection<Object>) Class.forName(className).newInstance();
	int size = WritableUtils.readVInt(in);
	if (BaseTypeUtil.isBaseType(ownerType)) {
	    for(int i = 0;i < size;i ++){
		coll.add(BaseTypeUtil.getValue(ownerType, in));
	    }
	}else if (Writable.class.isAssignableFrom(ownerType)) {
	    for(int i = 0;i < size;i ++){
		coll.add(readObject(in));
	    }
	} else if (Enum.class.isAssignableFrom(ownerType)) {
	    for(int i = 0;i < size;i ++){
		coll.add(readEnum(in));
	    }
	}else {
	    throw new IOException("Type : \"" + ownerType + "\" is unsupport");
	}
	in.close();
	return coll;
    }

    /**
     * 
     * @param writable
     * @param out
     * @throws IOException
     */
    public static final void writeObject(Writable writable,DataOutput out) throws IOException{
	WritableUtils.writeString(out, writable.getClass().getName());
	writable.write(out);
    }
    
    /**
     * 
     * @param value
     * @param out
     * @throws IOException
     */
    public static final void writeEnum(Enum<?> value,DataOutput out) throws IOException{
	WritableUtils.writeString(out, value.getClass().getName());
	WritableUtils.writeEnum(out, value);
    }
    
    /**
     * 
     * @param value
     * @return
     * @throws IOException
     */
    public static final byte[] writeEnum(Enum<?> value) throws IOException{
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream data = new DataOutputStream(out);
	writeEnum(value,data);
	data.close();
	return out.toByteArray();
    }
    
    /**
     * 
     * @param data
     * @return
     * @throws Exception
     */
    public static final Enum<?> readEnum(byte[] data) throws Exception{
	DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
	Enum<?> obj = readEnum(in);
	in.close();
	return obj;
    }
    
    /**
     * 
     * @param in
     * @return
     * @throws Exception
     */
    public static final Enum<?> readEnum(DataInput in) throws Exception{
	String enumType = WritableUtils.readString(in);
	Class<?> enumClass = Class.forName(enumType);
	return WritableUtils.readEnum(in, (Class<? extends Enum>)enumClass);
    }
    
    /**
     * 
     * @param writable
     * @return
     * @throws IOException
     */
    public static final byte[] writeObject(Writable writable) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream data = new DataOutputStream(out);
	writeObject(writable,data);
	data.close();
	return out.toByteArray();
    }

    /**
     * 
     * @param in
     * @return
     * @throws Exception
     */
    public static final Writable readObject(DataInput in)throws Exception {
	String className = WritableUtils.readString(in);
	Writable obj = (Writable) Class.forName(className).newInstance();
	obj.readFields(in);
	return obj;
    }
    
    /**
     * 
     * @param data
     * @return
     * @throws Exception
     */
    public static final Writable readObject(byte[] data) throws Exception {
	DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
	String clazz = WritableUtils.readString(in);
	Writable obj = (Writable) Class.forName(clazz).newInstance();
	obj.readFields(in);
	return obj;
    }

    /**
     * 
     * @param writable
     * @return
     * @throws IOException
     */
    public static final byte[] toBytes(Writable writable) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream data = new DataOutputStream(out);
	writable.write(data);
	data.close();
	return out.toByteArray();
    }

    /**
     * 
     * @param writable
     * @param data
     * @throws IOException
     */
    public static final void read(Writable writable, byte[] data)
	    throws IOException {
	DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
	writable.readFields(in);
	in.close();
    }

    /**
     * 
     * @param writable
     * @param in
     * @throws IOException
     */
    public static final void read(Writable writable, InputStream in)
	    throws IOException {
	DataInputStream dIn = new DataInputStream(in);
	writable.readFields(dIn);
	dIn.close();
    }

    /**
     * 
     * @param writable
     * @param in
     * @throws IOException
     */
    public static final void read(Writable writable, DataInput in)
	    throws IOException {
	writable.readFields(in);
    }

    public static final void createWrite(Field[] fields) {
	for (Field f : fields) {
	    String name = f.getName();
	    Class<?> type = f.getType();
	    Type t = BaseTypeUtil.getType(type);
	    switch (t) {
	    case STRING:
		System.out.println("WritableUtils.writeString(out, " + name
			+ ");");
		break;
	    case INT:
		System.out.println("WritableUtils.writeVInt(out, " + name
			+ ");");
		break;
	    case FLOAT:
		System.out.println("out.writeFloat(" + name + ");");
		break;
	    case LONG:
		System.out.println("WritableUtils.writeVLong(out, " + name
			+ ");");
		break;
	    case DOUBLE:
		System.out.println("out.writeDouble(" + name + ");");
		break;
	    case SHORT:
		System.out.println("out.writeShort(" + name + ");");
		break;
	    case BOOLEAN:
		System.out.println("out.writeBoolean(" + name + ");");
		break;
	    default:
		break;
	    }
	}
    }

    public static final void createRead(Field[] fields) {
	for (Field f : fields) {
	    String name = f.getName();
	    Class<?> type = f.getType();
	    Type t = BaseTypeUtil.getType(type);
	    switch (t) {
	    case STRING:
		System.out.println(name + " = WritableUtils.readString(in);");
		break;
	    case INT:
		System.out.println(name + " = WritableUtils.readVInt(in);");
		break;
	    case FLOAT:
		System.out.println(name + " = in.readFloat(in);");
		break;
	    case LONG:
		System.out.println(name + " = WritableUtils.readVLong(in);");
		break;
	    case DOUBLE:
		System.out.println(name + " = in.readDouble(in);");
		break;
	    case SHORT:
		System.out.println(name + " = in.readShort(in);");
		break;
	    case BOOLEAN:
		System.out.println(name + " = in.readBoolean(in);");
		break;
	    default:
		break;
	    }
	}
    }
    
    /**
     * 
     * @param writable
     * @return
     * @throws IOException
     */
    public static final String toString(Writable writable) throws IOException{
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dos = new DataOutputStream(out);
	writable.write(dos);
	return Base64.encodeBytes(out.toByteArray());
    }
    
    /**
     * 
     * @param writable
     * @param value
     * @throws IOException
     */
    public static final void readString(Writable writable,String value) throws IOException{
	byte[] b = Base64.decode(value);
	ByteArrayInputStream in = new ByteArrayInputStream(b);
	DataInputStream dataIn = new DataInputStream(in);
	writable.readFields(dataIn);
    }

    public static final void createReadAndWrite(Class<?> class1) {
	Field[] fields = class1.getDeclaredFields();
	createRead(fields);
	System.out.println("===================");
	createWrite(fields);
    }
}
