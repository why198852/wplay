package com.wplay.hbase.dao;

import com.wplay.hbase.HbaseAdpter;
import com.wplay.hbase.util.HbaseUtil;
import com.wplay.hbase.vo.IncrementVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Row key 自增 DAO
 *
 * @author James
 *
 * @param <V>
 */
public abstract class IncrementDAO<V extends IncrementVO> extends HbaseAdpter<V> {

    public static final byte[] AUTO_INC_ROWKEY = Bytes.toBytes("##ROWKEY##"); // 自增rowkey
    public static final byte[] ROWKEY_QUALIFIER = Bytes.toBytes("Q"); // 自增rowkey的
								      // qualifier

    /**
     *
     * @param conf
     */
    public IncrementDAO(Configuration conf) {
	this(conf, null);
    }

    /**
     *
     * @param conf
     * @param tableName
     */
    public IncrementDAO(Configuration conf, String tableName) {
	super(conf, tableName);
    }

    /**
     *
     * @param v
     * @return
     * @throws IOException
     */
    public long add(V v) throws IOException {
	long rowkey = this.autoRowkey();
	v.setId(rowkey);
	Put put = parse(v,Bytes.toBytes(rowkey));
	HTableInterface htable = getHTable();
	try {
	    htable.put(put);
	    return rowkey;
	} finally {
	    this.release(htable);
	}
    }

    /**
     *
     * @param v
     * @throws IOException
     */
    public void update(V v)throws IOException {
    	long rowkey = v.getId();
    	if(rowkey == 0){
    		throw new IOException("The v is not has ID .");
    	}
    	Put put = parse(v,Bytes.toBytes(rowkey));
    	HTableInterface htable = getHTable();
    	try {
    	    htable.put(put);
    	} finally {
    	    this.release(htable);
    	}
    }

    /**
     *
     * @param id
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public void update(long id, byte[] family, byte[] qualifier,
            byte[] value) throws IOException {
        super.update(Bytes.toBytes(id), family, qualifier, value);
    }

    /**
     *
     * @param v
     * @param rowkey
     * @return
     * @throws IOException
     */
    protected Put parse(V v,byte[] rowkey) throws IOException {
	if (v != null) {
	    try {
		Put put = HbaseUtil.convertToPut(v, rowkey, getFamily());
		return put;
	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}
	return null;
    }

    /**
     *
     */
    protected Put parse(V v) throws IOException {
	return this.parse(v, generateRowKey());
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

    private long autoRowkey() throws IOException {
	return this.increment(AUTO_INC_ROWKEY, getFamily(),ROWKEY_QUALIFIER, 1l);
    }

    /**
     *
     * @return
     * @throws IOException
     */
    private byte[] generateRowKey() throws IOException {
	return Bytes.toBytes(this.autoRowkey());
    }

    /**
     *
     * @return
     */
    protected abstract byte[] getFamily();

    /**
     * 初始化对象
     * 
     * @return
     */
    protected abstract V initV();
}
