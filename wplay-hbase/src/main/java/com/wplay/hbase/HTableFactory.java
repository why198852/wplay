package com.wplay.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public class HTableFactory implements Serializable,Closeable {
    private static final long serialVersionUID = 1L;
    private static HTableFactory htableFactory;
    private static final int MAXCONN = 1000;
    private Configuration conf;
    private HTablePool htablePool;

    /**
     * 
     * @param conf
     */
    private HTableFactory(Configuration conf) {
	this(conf, MAXCONN);
    }

    /**
     * 
     * @param conf
     * @param maxConn
     */
    private HTableFactory(Configuration conf, int maxConn) {
	this.conf = conf;
	maxConn = conf.getInt("com.skycloud.hbase.maxconn", MAXCONN);
	this.htablePool = new HTablePool(conf, maxConn);
    }

    /**
     * 
     * @param tableName
     * @return
     */
    public HTableInterface getHTable(String tableName) {
	return this.htablePool.getTable(tableName.getBytes());
    }

    /**
     * 
     * @param htable
     */
    public void release(HTableInterface htable) {
	if (htable != null)
	    try {
		htable.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
    }

    /**
     * 
     * @param tableName
     * @param falmliys
     * @throws IOException
     */
    public void createTable(String tableName, String[] falmliys)
	    throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	HTableDescriptor tableDescripter = new HTableDescriptor(
		Bytes.toBytes(tableName));
	for (String fam : falmliys) {
	    HColumnDescriptor hcolumn = new HColumnDescriptor(
		    Bytes.toBytes(fam));
	    hcolumn.setCompressionType(Algorithm.GZ);
	    hcolumn.setBloomFilterType(BloomType.ROW);
	    tableDescripter.addFamily(hcolumn);
	}
	admin.createTable(tableDescripter);
	admin.close();
    }

    /**
     * 
     * @param tableName
     * @param falmliys
     * @param startRow
     * @param stopRow
     * @param numRegions
     * @throws IOException
     */
    public void createTable(String tableName, String[] falmliys,
	    String startRow, String stopRow, int numRegions) throws IOException {
	this.createTable(tableName, falmliys, Bytes.toBytes(startRow),
		Bytes.toBytes(stopRow), numRegions, Algorithm.GZ);
    }

    /**
     * 
     * @param tableName
     * @param falmliys
     * @param startRow
     * @param stopRow
     * @param numRegions
     * @throws IOException
     */
    public void createTable(String tableName, String[] falmliys,
	    String startRow, String stopRow, int numRegions, Algorithm compres)
	    throws IOException {
	this.createTable(tableName, falmliys, Bytes.toBytes(startRow),
		Bytes.toBytes(stopRow), numRegions, compres);
    }

    /**
     * 
     * @param tableName
     * @param falmliys
     * @param startRow
     * @param stopRow
     * @param numRegions
     * @throws IOException
     */
    public void createTable(String tableName, String[] falmliys,
	    byte[] startRow, byte[] stopRow, int numRegions, Algorithm compres)
	    throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	HTableDescriptor tableDescripter = new HTableDescriptor(
		Bytes.toBytes(tableName));
	for (String fam : falmliys) {
	    HColumnDescriptor hcolumn = new HColumnDescriptor(
		    Bytes.toBytes(fam));
	    hcolumn.setCompressionType(compres);
	    hcolumn.setBloomFilterType(BloomType.ROW);
	    hcolumn.setMaxVersions(1);
	    tableDescripter.addFamily(hcolumn);
	}
	admin.createTable(tableDescripter, startRow, stopRow, numRegions);
	admin.close();
    }

    /**
     * 
     * @param tableName
     * @param falmliys
     * @throws IOException
     */
    public void createTable(String tableName, byte[][] falmliys)
	    throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	HTableDescriptor tableDescripter = new HTableDescriptor(
		Bytes.toBytes(tableName));
	for (byte[] fam : falmliys) {
	    HColumnDescriptor hcolumn = new HColumnDescriptor(fam);
	    hcolumn.setCompressionType(Algorithm.GZ);
	    hcolumn.setBloomFilterType(BloomType.ROW);
	    tableDescripter.addFamily(hcolumn);
	}
	admin.createTable(tableDescripter);
	admin.close();
    }

    /**
     * 
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(byte[] tableName) throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	try{
		return admin.tableExists(tableName);
	}finally{
		admin.close();
	}
    }

    /**
     * 
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(String tableName) throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	try{
		return admin.tableExists(tableName);
	}finally{
		admin.close();
	}
    }

    /**
     * É¾³ýtable
     *
     * @param tableName
     * @throws IOException
     */
    public void drop(String tableName) throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	admin.disableTable(tableName);
	admin.deleteTable(tableName);
	admin.close();
    }

    /**
     * É¾³ýtable
     * 
     * @param tableName
     * @throws IOException
     */
    public void drop(byte[] tableName) throws IOException {
	HBaseAdmin admin = new HBaseAdmin(this.conf);
	admin.disableTable(tableName);
	admin.deleteTable(tableName);
	admin.close();
    }

    public static synchronized HTableFactory getHTableFactory(
	    Configuration conf, int maxConn) {
	if (htableFactory == null) {
	    htableFactory = new HTableFactory(conf, maxConn);
	}
	return htableFactory;
    }

    public static HTableFactory getHTableFactory(Configuration conf) {
	return getHTableFactory(conf, MAXCONN);
    }

    public Configuration getConf() {
	return conf;
    }

    
    
    public void setConf(Configuration conf) {
	this.conf = conf;
    }

    @Override
    public void close() throws IOException {
	if(this.htablePool != null){
	    this.htablePool.close();
	}
    }
}