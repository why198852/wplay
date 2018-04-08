package com.wplay.hbase.delete;

import com.wplay.hbase.HbaseOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author James
 */
public class HbaseDelete extends HbaseOption {

    /**
     * 
     * @param conf
     * @param tableName
     */
    public HbaseDelete(Configuration conf, String tableName) {
	super(conf, tableName);
    }

    /**
     * 
     * @param rowKey
     * @throws IOException
     */
    public void delete(byte[] rowKey) throws IOException {
	Delete delete = new Delete(rowKey);
	HTableInterface htable = getHTable();
	try {
	    htable.delete(delete);
	} finally {
	    release(htable);
	}
    }

    /**
     * 
     * @param rowKey
     * @throws IOException
     */
    public void delete(String rowKey) throws IOException {
	this.delete(Bytes.toBytes(rowKey));
    }

    /**
     * 
     * @param rows
     * @throws IOException
     */
    public void delete(List<String> rows) throws IOException {
	List<Delete> deletes = new ArrayList<Delete>();
	for (String row : rows) {
	    deletes.add(new Delete(Bytes.toBytes(row)));
	}
	HTableInterface htable = getHTable();
	try {
	    htable.delete(deletes);
	} finally {
	    release(htable);
	}
    }
}
