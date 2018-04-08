package com.wplay.hbase.reader;

import org.apache.hadoop.hbase.client.Put;

import java.util.List;

public abstract interface HbaseGetter {
	public abstract Object getValue(String paramString1, String paramString2,
                                    String paramString3, String paramString4, Class paramClass);

	public abstract String getValue(String paramString1, String paramString2,
                                    String paramString3, String paramString4);

	public abstract void putValue(String paramString1, String paramString2,
                                  String paramString3, String paramString4, byte[] paramArrayOfByte);

	public abstract void pubValue(String paramString, Put paramPut);

	public abstract void putValue(String paramString, List<Put> paramList);
}