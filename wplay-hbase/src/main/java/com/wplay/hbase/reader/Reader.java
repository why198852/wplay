package com.wplay.hbase.reader;

import java.io.IOException;
import java.util.List;

/**
 * 
 * Hbase 读取
 * @author James
 *
 * @param <V>
 */
public interface Reader<V> {

	/**
	 *
	 * @param tableName 表名
	 * @param startRow 起始行的值
	 * @param limit 每页的个数
	 * @return
	 * @throws Exception
	 */
	List<V> read(String tableName, String startRow, long limit) throws Exception;

	/**
	 *
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @return
	 * @throws Exception
	 */
	List<V> read(String tableName, String startRow, String stopRow) throws Exception;

	/**
	 * 前缀搜索
	 * @param tableName 表名
	 * @param prefix rowKey前缀
	 * @param startRow 
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	List<V> query(String tableName, String prefix, String startRow, long limit) throws Exception;
	
	/**
	 * 
	 * @param tableName
	 * @param startRow
	 * @param prefix
	 * @param currentPage 
	 * @param goPage
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	List<V> query(String tableName, String startRow, String prefix, long currentPage, long goPage, long limit)throws Exception;
	
	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws Exception
	 */
	V get(String tableName, String rowKey) throws Exception;
	
	/**
	 * 
	 * @param tableName
	 * @param startRow
	 * @param currentPage
	 * @param goPage
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	List<V> goPage(String tableName, String startRow, long currentPage, long goPage, long limit)throws Exception;
	
	/**
	 * 
	 * @param tableName
	 * @param prefix
	 * @return
	 * @throws Exception
	 */
	long queryTotal(String tableName, String prefix)throws Exception;
	
	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public long getTotal(String tableName) throws IOException;
	
	/**
	 * 
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @param start
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public List<V> goPage(String tableName, String startRow, String endRow, long start, long limit) throws Exception;

	/**
	 * 
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @return
	 * @throws Exception
	 */
	public long queryTotal(String tableName, String startRow, String endRow) throws Exception;
}