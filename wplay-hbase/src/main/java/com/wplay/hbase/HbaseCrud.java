package com.wplay.hbase;

import java.io.IOException;
import java.util.List;

/**
 * Hbase 增删改操作
 * @author James
 *
 */
public interface HbaseCrud {

	/**
	 * 创建表
	 * @param tableName
	 * @param famliys
	 * @throws IOException
	 */
	void createTalbe(String tableName, String[] famliys) throws IOException;

	/**
	 * 删除表
	 * @param tableName
	 * @throws IOException
	 */
	void dorpTable(String tableName) throws IOException;

	/**
	 * 插入一行
	 * @param tableName
	 * @param rowKey
	 * @param famliy
	 * @param qualifier
	 * @param value
	 */
	void insertRow(String tableName, String rowKey, String famliy, String qualifier, Object value) throws IOException;

	/**
	 * 删除一行
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	void deleteRow(String tableName, String rowKey) throws IOException;

	/**
	 * 删除多行
	 * @param tableName
	 * @param rowKeys
	 * @throws IOException
	 */
	void deleteRow(String tableName, List<String> rowKeys) throws IOException;

	/**
	 *
	 * @param tableName
	 * @param rowkey
	 * @param famliy
	 * @param qualifier
	 * @throws IOException
	 */
	void deleteCell(String tableName, String rowkey, String famliy, String qualifier) throws IOException;

	/**
	 * 更新cell
	 * @param tableName
	 * @param famliy
	 * @param qualifier
	 * @param value
	 * @throws IOException
	 */
	void updateCell(String tableName, String rowkey, String famliy, String qualifier, Object value)  throws IOException;
	
}
