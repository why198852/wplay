package com.wplay.hbase.util;

import com.wplay.hbase.annotation.NoSave;
import com.wplay.hbase.reflect.FieldType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * 
 * @author James
 * 
 */
public class HbaseUtil {

    /**
     * 
     * @param scan
     * @return
     * @throws IOException
     */
    @Deprecated
    public static String scanToString(Scan scan) throws IOException {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dos = new DataOutputStream(out);
	//scan.write(dos);
	return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * 
     * @param obj
     * @param family
     * @return
     * @throws Exception
     */
    public static final Put convertToPut(Object obj, byte[] rowKey,
	    byte[] family) throws Exception {
	Class<?> clazz = obj.getClass();
	List<FieldType> fts = getFieldTypes(clazz);
	Put put = new Put(rowKey);
	for (FieldType ft : fts) {
	    String name = ft.getName();
	    java.lang.reflect.Type type = ft.getType();
	    Method method = ft.getPd().getReadMethod();
	    Object value = method.invoke(obj, null);
	    if (type instanceof Class) {
		if (value != null) {
		    byte[] v = null;
		    if (BaseTypeUtil.isBaseType((Class<?>) type)) {
			v = BaseTypeUtil.toBytes(value);
		    } else if (Writable.class.isAssignableFrom((Class<?>) type)) {
			v = WriteableUtil.writeObject((Writable) value);
		    } else if (Enum.class.isAssignableFrom((Class<?>) type)) {
			v = WriteableUtil.writeEnum((Enum<?>) value);
		    } else {
			throw new IOException("Type : \"" + type
				+ "\" is unsupport");
		    }
		    put.add(family, Bytes.toBytes(name), v);
		}
	    } else {
		if (type instanceof ParameterizedType) {
		    ParameterizedType pt = (ParameterizedType) type;
		    Class<?> ownerType = (Class<?>) pt.getActualTypeArguments()[0];
		    Collection<?> coll = (Collection<?>) value;
		    byte[] b = WriteableUtil.writeCollection(coll, ownerType);
		    put.add(family, Bytes.toBytes(name), b);
		} else if (type instanceof GenericArrayType) {
		    GenericArrayType gat = (GenericArrayType) type;
		    Class<?> componentType = (Class<?>) gat
			    .getGenericComponentType();
		    byte[] b = WriteableUtil.writeArray(value, componentType);
		    put.add(family, Bytes.toBytes(name), b);
		} else {
		    throw new IOException("Type : \"" + type
			    + "\" is unsupport");
		}
	    }
	}
	return put;
    }

    /**
     * 
     * @param result
     * @param obj
     * @return
     * @throws Exception
     */
    public static final void fillObj(Result result, Object obj)
	    throws Exception {
	// byte rowKey[] = result.getRow();
	// setValue(obj, "rowKey", rowKey); // 设置RowKey
	for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result
		.getMap().entrySet()) {
	    // byte family[] = entry.getKey();
	    // String fam = Bytes.toString(family);
	    NavigableMap<byte[], NavigableMap<Long, byte[]>> value = entry
		    .getValue();
	    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> chEntry : value
		    .entrySet()) {
		byte qualifier[] = chEntry.getKey();
		NavigableMap<Long, byte[]> chValue = chEntry.getValue();
		String field = Bytes.toString(qualifier);
		setValue(obj, field, chValue.lastEntry().getValue());
	    }
	}
    }

    /**
     *
     * @param v
     * @param field
     * @param value
     * @throws Exception
     */
    private static final void setValue(Object v, String field, byte[] value)
	    throws Exception {
	Class<?> clazz = v.getClass();
	FieldType ft = getFieldType(clazz, field);
	if (ft != null) {
	    if (ft.getType() instanceof Class) {
		Class<?> c = (Class<?>) ft.getType();
		Method method = ft.getPd().getWriteMethod();
		Object o = null;
		if (BaseTypeUtil.isBaseType((Class<?>) c)) {
		    o = BaseTypeUtil.getValue(c, value);
		} else if (Writable.class.isAssignableFrom(c)) {
		    o = WriteableUtil.readObject(value);
		} else if (Enum.class.isAssignableFrom(c)) {
		    o = WriteableUtil.readEnum(value);
		} else {
		    throw new IOException("Type : \"" + ft.getType() + "\" is unsupport");
		}
		if (o != null) {
		    method.invoke(v, o);
		}
	    } else {
		if (ft.getType() instanceof ParameterizedType) {
		    ParameterizedType pt = (ParameterizedType) ft.getType();
		    Class<?> ownerType = (Class<?>) pt.getActualTypeArguments()[0];
		    Object obj = WriteableUtil.readCollection(value, ownerType);
		    Method method = ft.getPd().getWriteMethod();
		    if (obj != null) {
			method.invoke(v, obj);
		    }
		} else if (ft.getType() instanceof GenericArrayType) {
		    GenericArrayType gat = (GenericArrayType) ft.getType();
		    Class<?> componentType = (Class<?>) gat
			    .getGenericComponentType();
		    Object obj = WriteableUtil.readArray(value, componentType);
		    Method method = ft.getPd().getWriteMethod();
		    if (obj != null) {
			method.invoke(v, obj);
		    }
		} else {
		    throw new IOException("Type : \"" + ft.getType()  + "\" is unsupport");
		}
	    }
	}
    }

    /**
     *
     * @param clazz
     * @return
     * @throws Exception
     */
    public static List<Method> getReadMethods(Class<?> clazz) throws Exception {
	List<Method> methods = new ArrayList<Method>();
	List<FieldType> fts = getFieldTypes(clazz);
	for (FieldType ft : fts) {
	    methods.add(ft.getPd().getReadMethod());
	}
	return methods;
    }

    /**
     *
     * @param clazz
     * @return
     * @throws Exception
     */
    public static List<Method> getWriteMethods(Class<?> clazz) throws Exception {
	List<Method> methods = new ArrayList<Method>();
	List<FieldType> fts = getFieldTypes(clazz);
	for (FieldType ft : fts) {
	    methods.add(ft.getPd().getWriteMethod());
	}
	return methods;
    }

    /**
     *
     * @param clazz
     * @return
     * @throws Exception
     */
    public static List<FieldType> getFieldTypes(Class<?> clazz)
	    throws Exception {
	List<FieldType> fts = new ArrayList<FieldType>();
	Method[] fields = clazz.getMethods();// 获得属性
	for (Method method : fields) {
	    String name = method.getName();
	    if ((name.startsWith("get") || name.startsWith("is")) && !name.equals("getClass")) {
		NoSave noSave = method.getAnnotation(NoSave.class);
		if (noSave == null) {
		    String field = toAttribute(name, "get|is");
		    try {
			FieldType ft = new FieldType();
			ft.setName(field);
			ft.setPd(new PropertyDescriptor(field, clazz));
			ft.setType(method.getGenericReturnType());
			fts.add(ft);
		    } catch (Exception e) {
		    }
		}
	    }
	}
	return fts;
    }

    private static final String toAttribute(String method, String type) {
	method = method.replaceAll("^" + type, "");
	char v = method.charAt(0);
	return method.replaceAll("^" + v,
		String.valueOf(Character.toLowerCase(v)));
    }

    /**
     * 
     * @param clazz
     * @return
     * @throws Exception
     */
    public static FieldType getFieldType(Class<?> clazz, String field)
	    throws Exception {
	FieldType ft = new FieldType();
	PropertyDescriptor pd = new PropertyDescriptor(field, clazz);
	ft.setPd(pd);
	ft.setName(field);
	ft.setType(pd.getReadMethod().getGenericReturnType());
	return ft;
    }

    /**
     * 
     * @param clazz
     * @param field
     * @return
     */
    public static PropertyDescriptor getPropertyDescriptor(Class<?> clazz,
	    String field) {
	try {
	    PropertyDescriptor pd = new PropertyDescriptor(field, clazz);
	    return pd;
	} catch (Exception e) {
	    return null;
	}
    }
}
