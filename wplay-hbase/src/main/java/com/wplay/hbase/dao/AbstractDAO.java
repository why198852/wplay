package com.wplay.hbase.dao;

import com.wplay.hbase.HbaseAdpter;
import com.wplay.hbase.util.HbaseUtil;
import com.wplay.hbase.util.HbaseWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public abstract class AbstractDAO<V extends HbaseWritable> extends
	HbaseAdpter<V> {
    public static final String family = "I";
    public static final String qualifier = "Q";

    public static final byte[] B_F = Bytes.toBytes(family);
    public static final byte[] B_Q = Bytes.toBytes(qualifier);
    public static final String DELETED = "_delete";

    public AbstractDAO(Configuration conf, String tableName) {
	super(conf, tableName);
    }

    @Override
    protected Put parse(V v) throws IOException {
	if (v != null) {
	    try {
		Put put = HbaseUtil.convertToPut(v, v.getRowKey(),B_F);
		return put;
	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}
	return null;
    }

    @Override
    protected V parse(Result result) throws IOException {
	if (result != null && !result.isEmpty()) {
	    V v = initV();
	    try {
		HbaseUtil.fillObj(result, v);
		return v;
	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}
	return null;
    }

    /**
     * 初始化对象
     * 
     * @return
     */
    protected abstract V initV();
}
