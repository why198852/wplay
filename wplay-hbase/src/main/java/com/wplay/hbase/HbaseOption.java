package com.wplay.hbase;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.Closeable;
import java.io.IOException;

/**
 * 
 * @author James
 * 
 */
public abstract class HbaseOption extends Configured implements Configurable,Closeable {

    private HTableFactory htableFactroy;
    private String tableName;

    /**
     * 
     * @param conf
     */
    public HbaseOption(Configuration conf){
	this(conf,null);
    }
    
    /**
     * 
     * @param conf
     * @param tableName
     */
    public HbaseOption(Configuration conf, String tableName) {
	super(conf);
	htableFactroy = HTableFactory.getHTableFactory(conf);
	this.setTableName(tableName);
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
	return tableName;
    }

    /**
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName) {
	this.tableName = tableName;
    }

    /**
     * 
     * @return
     */
    public HTableInterface getHTable() {
	return htableFactroy.getHTable(tableName);
    }

    /**
     * 
     * @param htable
     */
    public void release(HTableInterface htable) {
	htableFactroy.release(htable);
    }
    
    @Override
    public void close() throws IOException {
	if(this.htableFactroy != null){
	    this.htableFactroy.close();
	    this.htableFactroy = null;
	}
    }
}
