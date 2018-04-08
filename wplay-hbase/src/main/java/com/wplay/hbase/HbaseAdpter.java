package com.wplay.hbase;

import com.wplay.hbase.delete.HbaseDelete;
import com.wplay.hbase.insert.HbaseInsert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * 
 * @author James
 * 
 * @param <V>
 */
public abstract class HbaseAdpter<V> extends HbaseInsert<V> {

    private HbaseDelete delete;

    /**
     *   
     * @param conf
     */
    public HbaseAdpter(Configuration conf) {
	super(conf, null);
    }

    /**
     * 
     * @param conf
     * @param tableName
     */
    public HbaseAdpter(Configuration conf, String tableName) {
	super(conf, tableName);
	this.delete = new HbaseDelete(conf, tableName);
    }

    /**
     * 
     * @param rowKey
     * @throws IOException
     */
    public void delete(byte[] rowKey) throws IOException {
	delete.delete(rowKey);
    }

    /**
     * 
     * @param rowKey
     * @throws IOException
     */
    public void delete(String rowKey) throws IOException {
	delete.delete(rowKey);
    }

    /**
     * 
     * @param rows
     * @throws IOException
     */
    public void delete(List<String> rows) throws IOException {
	delete.delete(rows);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        if(this.delete != null){
            this.delete.close();
            this.delete = null;
        }
    }
}
