package com.wplay.hbase.insert;

import com.wplay.hbase.query.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author James
 * 
 * @param <V>
 */
public abstract class HbaseInsert<V> extends Query<V> {

    public HbaseInsert(Configuration conf){
	this(conf,null);
    }
    
    /**
     * 
     * @param conf
     * @param tableName
     */
    public HbaseInsert(Configuration conf, String tableName) {
	super(conf, tableName);
    }

    /**
     * 
     * @param v
     * @throws IOException
     */
    public void insert(V v) throws IOException {
	Put put = parse(v);
	HTableInterface htable = getHTable();
	try {
	    htable.put(put);
	} finally {
	    this.release(htable);
	}
    }

    /**
     * 
     * @param rowKey
     * @param famliy
     * @param qualifier
     * @param increment
     * @return
     * @throws IOException
     */
    public long increment(String rowKey, String famliy, String qualifier,
	    long increment) throws IOException {
	return this.increment(Bytes.toBytes(rowKey), Bytes.toBytes(famliy),
		Bytes.toBytes(qualifier), increment);
    }

    /**
     * 
     * @param rowKey
     * @param famliy
     * @param qualifier
     * @param increment
     * @return
     * @throws IOException
     */
    public long increment(String rowKey, byte[] famliy, byte[] qualifier,
	    long increment) throws IOException {
	return this.increment(Bytes.toBytes(rowKey), famliy, qualifier,
		increment);
    }

    /**
     * 
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public void update(String rowKey,byte[] family, byte[] qualifier,byte[] value) throws IOException{
	this.update(Bytes.toBytes(rowKey), family, qualifier, value);
    }
    
    /**
     * 
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public void update(byte[] rowKey,byte[] family, byte[] qualifier,byte[] value) throws IOException{
	Put put = new Put(rowKey);
	put.add(family, qualifier, value);
	HTableInterface htable = getHTable();
	try {
	    htable.put(put);
	} finally {
	    this.release(htable);
	}
    }
    
    /**
     * 
     * @param rowKey
     * @param famliy
     * @param qualifier
     * @param increment
     * @return
     * @throws IOException
     */
    public long increment(byte[] rowKey, byte[] famliy, byte[] qualifier,
	    long increment) throws IOException {
	HTableInterface htable = getHTable();
	try {
	    return htable.incrementColumnValue(rowKey, famliy, qualifier,
		    increment);
	} finally {
	    release(htable);
	}
    }

    /**
     * 
     * @param vs
     * @throws IOException
     */
    public void insert(List<V> vs) throws IOException {
	List<Put> puts = new ArrayList<Put>();
	for (V v : vs) {
	    puts.add(parse(v));
	}
	HTableInterface htable = getHTable();
	try {
	    htable.put(puts);
	} finally {
	    release(htable);
	}
    }

    /**
     * 
     * @param v
     * @return
     */
    protected abstract Put parse(V v) throws IOException;
}
