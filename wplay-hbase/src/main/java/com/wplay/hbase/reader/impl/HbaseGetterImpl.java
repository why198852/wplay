package com.wplay.hbase.reader.impl;

import com.wplay.hbase.HTableFactory;
import com.wplay.hbase.reader.HbaseGetter;
import com.wplay.hbase.util.BaseTypeUtil;
import com.wplay.core.util.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseGetterImpl implements HbaseGetter {
	private HTableFactory htFactory;

	public HbaseGetterImpl(HTableFactory htFactory) {
		this.htFactory = htFactory;
	}

	public HbaseGetterImpl(Configuration conf) {
		this(HTableFactory.getHTableFactory(conf));
	}

	public Object getValue(String tableName, String rowKey, String family,
			String qualifier, Class reType) {
		HTableInterface htable = this.htFactory.getHTable(tableName);
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = htable.get(get);
			byte[] b = result.getValue(Bytes.toBytes(family),
					Bytes.toBytes(qualifier));
			if (b == null) {
				return null;
			}
			return BaseTypeUtil.getValue(reType, b);
		} catch (IOException e) {
			LogUtil.error(getClass(), e.getMessage());
			Result result = null;
			return result;
		} finally {
			this.htFactory.release(htable);
		}
	}

	public String getValue(String tableName, String rowKey, String family,
			String qualifier) {
		return (String) getValue(tableName, rowKey, family, qualifier,
				String.class);
	}

	public void putValue(String tableName, String rowKey, String family,
			String qualifier, byte[] value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
		pubValue(tableName, put);
	}

	public void pubValue(String tableName, Put put) {
		HTableInterface htable = this.htFactory.getHTable(tableName);
		try {
			htable.put(put);
		} catch (IOException e) {
			LogUtil.error(getClass(), e.getMessage());
		} finally {
			this.htFactory.release(htable);
		}
	}

	public void putValue(String tableName, List<Put> puts) {
		HTableInterface htable = this.htFactory.getHTable(tableName);
		try {
			htable.put(puts);
		} catch (IOException e) {
			LogUtil.error(getClass(), e.getMessage());
		} finally {
			this.htFactory.release(htable);
		}
	}
}