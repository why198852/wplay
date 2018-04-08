package com.wplay.hbase.impl;

import com.wplay.hbase.HbaseCrud;
import com.wplay.hbase.HTableFactory;
import com.wplay.hbase.util.BaseTypeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 *  Hbase 增删改的实现
 * @author James
 *
 */
public class HbaseCrudImpl implements HbaseCrud{

	private HTableFactory htableFactory;
	private Configuration conf;
	
	public HbaseCrudImpl(Configuration conf){
		this(HTableFactory.getHTableFactory(conf));
	}
	
	public HbaseCrudImpl(HTableFactory htableFactory){
		this.htableFactory = htableFactory;
		this.conf = htableFactory.getConf();
	}
	
	@Override
	public void createTalbe(String tableName, String[] famliys)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(this.conf);
		HTableDescriptor tableDescripter = new HTableDescriptor(
				TableName.valueOf(tableName));
		for (String fam : famliys) {
			tableDescripter.addFamily(new HColumnDescriptor(Bytes.toBytes(fam)));
		}
		admin.createTable(tableDescripter);
	}

	@Override
	public void dorpTable(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(this.conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	@Override
	public void insertRow(String tableName, String rowKey, String famliy,
			String qualifier, Object value) throws IOException {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try{
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(famliy), Bytes.toBytes(qualifier), BaseTypeUtil.toBytes(value));
			htable.put(put);
		}finally{
			htableFactory.release(htable);
		}
	}

	@Override
	public void deleteRow(String tableName, String rowKey) throws IOException {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try{
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			htable.delete(delete);
		}finally{
			htableFactory.release(htable);
		}
	}

	@Override
	public void deleteRow(String tableName, List<String> rowKeys)
			throws IOException {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try{
			List<Delete> deletes = new LinkedList<Delete>();
			for(String rowKey : rowKeys){
				deletes.add(new Delete(Bytes.toBytes(rowKey)));
			}
			htable.delete(deletes);
		}finally{
			htableFactory.release(htable);
		}
	}

	@Override
	public void deleteCell(String tableName, String rowKey, String famliy,
			String qualifier) throws IOException {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try{
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.deleteColumn(Bytes.toBytes(famliy), Bytes.toBytes(qualifier));
			htable.delete(delete);
		}finally{
			htableFactory.release(htable);
		}
	}

	@Override
	public void updateCell(String tableName, String rowKey, String famliy,
			String qualifier, Object value) throws IOException {
		this.insertRow(tableName, rowKey, famliy, qualifier, value);
	}

}
