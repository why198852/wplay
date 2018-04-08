package com.wplay.hbase.query;

import com.wplay.hbase.HbaseOption;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询通用类
 *
 * @author James
 *
 */
public abstract class Query<V> extends HbaseOption implements Configurable {

    public static final String MIN_CHAR = "#";
    public static final String MAX_CHAR = "~";
    public static final long UNLIMIT = -1;
    public static final long NOSKIP = -1;

    public Query(Configuration conf) {
	this(conf,null);
    }

    /**
     *
     * @param conf
     * @param tableName
     */
    public Query(Configuration conf, String tableName) {
	super(conf, tableName);
    }

    /**
     *
     * @param rowKey
     * @return
     * @throws IOException
     */
    public V doGet(String rowKey) throws IOException {
	return doGet(Bytes.toBytes(rowKey));
    }

    public List<V> doGet(List<String> rowKeys) throws IOException {
	if(rowKeys != null){
	    List<Get> gets = new ArrayList<Get>();
	    for(String rowkey : rowKeys){
		Get get = new Get(Bytes.toBytes(rowkey));
		gets.add(get);
	    }
	    HTableInterface htable = getHTable();
	    try {
		Result[] results = htable.get(gets);
		if (results != null) {
		    List<V> re = new ArrayList<V>();
		    for (Result result : results) {
			re.add(parse(result));
		    }
		    return re;
		}
		return null;
	    } finally {
		release(htable);
	    }
	}
	return null;
    }

    /**
     *
     * @param rowKey
     * @return
     * @throws IOException
     */
    public V doGet(byte[] rowKey) throws IOException {
	Get get = new Get(rowKey);
	HTableInterface htable = getHTable();
	try {
	    Result result = htable.get(get);
	    if (result != null) {
		return parse(result);
	    }
	    return null;
	} finally {
	    release(htable);
	}
    }

    /**
     *
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public List<V> scan(byte[] startRow, byte[] stopRow) throws IOException {
	return scan(startRow, stopRow, UNLIMIT);
    }

    /**
     *
     * @param startRow
     * @param stopRow
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(byte[] startRow, byte[] stopRow, long limit)
	    throws IOException {
	return scan(startRow, stopRow, NOSKIP, limit);
    }

    /**
     *
     * @param startRow
     * @param stopRow
     * @param flter
     * @return
     * @throws IOException
     */
    public List<V> scan(byte[] startRow, byte[] stopRow, Filter flter)
	    throws IOException {
	return scan(startRow, stopRow, NOSKIP,UNLIMIT, flter);
    }

    /**
     *
     * @param startRow
     * @param stopRow
     * @param skip
     *            跳过前多少条
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(byte[] startRow, byte[] stopRow, long skip, long limit)
	    throws IOException {
	Scan scan = new Scan(startRow, stopRow);
	Filter filter = this.getPageFilter(skip, limit);
	if (filter != null) {
	    scan.setFilter(filter);
	}
	return scan(scan, skip);
    }

    /**
     *
     * @param startRow
     * @param stopRow
     * @param skip
     *            跳过前多少条
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(byte[] startRow, byte[] stopRow, long skip, long limit,Filter filter)
	    throws IOException {
	Scan scan = new Scan(startRow, stopRow);
	FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
	Filter pagerFilter = this.getPageFilter(skip, limit);
	if(pagerFilter != null){
	    filterList.addFilter(pagerFilter);
	}
	if(filter != null){
	    filterList.addFilter(filter);
	}
	
	if (!filterList.getFilters().isEmpty()) {
	    scan.setFilter(filterList);
	}
	return scan(scan, skip);
    }

    /**
     * 
     * @param scan
     * @return
     * @throws IOException
     */
    public List<V> scan(Scan scan, long skip) throws IOException {
	HTableInterface htable = getHTable();
	ResultScanner rs = null;
	try {
	    rs = htable.getScanner(scan);
	    List<V> results = new ArrayList<V>();
	    if (rs != null) {
		if (this.skip(rs, skip)) {
		    for (Result result : rs) {
			V v = parse(result);
			if (v != null) {
			    results.add(v);
			}
		    }
		}
	    }
	    return results.isEmpty() ? null : results;
	} finally {
	    release(htable);
	    if(rs != null){
		rs.close();
	    }
	}
    }

    /**
     * 
     * @param scan
     * @return
     * @throws IOException
     */
    public List<V> scan(Scan scan) throws IOException {
	return this.scan(scan, NOSKIP);
    }

    /**
     * 
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String stopRow) throws IOException {
	return scan(startRow, stopRow, UNLIMIT);
    }
    
    /**
     * 
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String stopRow,Filter filter) throws IOException {
	return scan(startRow, stopRow, UNLIMIT,filter);
    }

    /**
     * 
     * @param startRow
     * @param stopRow
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String stopRow, long limit)
	    throws IOException {
	return scan(startRow, stopRow, NOSKIP, limit);
    }
    
    /**
     * 
     * @param startRow
     * @param stopRow
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String stopRow, long limit,Filter filter)
	    throws IOException {
	return scan(startRow, stopRow, NOSKIP, limit,filter);
    }

    /**
     * 
     * @param startRow
     * @param endRow
     * @param skip
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String endRow, long skip, long limit)
	    throws IOException {
	return scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow), skip, limit);
    }
    
    /**
     * 
     * @param startRow
     * @param endRow
     * @param skip
     * @param limit
     * @return
     * @throws IOException
     */
    public List<V> scan(String startRow, String endRow, long skip, long limit,Filter filter)
	    throws IOException {
	return scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow), skip, limit,filter);
    }

    /**
     * 
     * @param result
     * @return
     * @throws IOException
     */
    protected abstract V parse(Result result) throws IOException;

    /**
     * 
     * @param skip
     * @param limit
     * @return
     */
    protected Filter getPageFilter(long skip, long limit) {
	if (limit >= 0) {
	    long pageSize = skip >= 0 ? limit + skip : limit;
	    Filter filter = new PageFilter(pageSize);
	    return filter;
	}
	return null;
    }

    /**
     * 
     * @param startRow
     * @param endRow
     * @return
     * @throws IOException
     */
    public long getTotal(String startRow, String endRow) throws IOException {
	return getTotal(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
    }

    /**
     * 
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public long getTotal(byte[] startRow, byte[] stopRow) throws IOException {
	Scan scan = new Scan(startRow, stopRow);
	return getTotal(scan);
    }

    /**
     * 
     * @param scan
     * @return
     * @throws IOException
     */
    public long getTotal(Scan scan) throws IOException {
	HTableInterface htable = getHTable();
	long total = 0;
	try {
	    ResultScanner rs = htable.getScanner(scan);
	    if (rs != null) {
		while (rs.next() != null) {
		    total++;
		}
	    }
	} finally {
	    release(htable);
	}
	return total;
    }

    /**
     * 
     * @param get
     * @return
     * @throws IOException
     */
    public Result getResult(Get get) throws IOException {
	HTableInterface htable = getHTable();
	try {
	    return htable.get(get);
	} finally {
	    release(htable);
	}
    }

    /**
     * 
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     * @throws IOException
     */
    public byte[] getValue(String rowKey, byte[] family, byte[] qualifier)
	    throws IOException {
	HTableInterface htable = getHTable();
	try {
	    Get get = new Get(Bytes.toBytes(rowKey));
	    get.addColumn(family, qualifier);
	    Result result = htable.get(get);
	    if (result != null && !result.isEmpty()) {
		return result.getValue(family, qualifier);
	    }
	    return null;
	} finally {
	    release(htable);
	}
    }

    /**
     * 
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Result getResult(byte[] rowKey) throws IOException {
	return getResult(new Get(rowKey));
    }

    /**
     * 
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Result getResult(String rowKey) throws IOException {
	return getResult(Bytes.toBytes(rowKey));
    }

    /**
     * 
     * @param rs
     * @param rows
     * @return
     * @throws IOException
     */
    private boolean skip(ResultScanner rs, long rows) throws IOException {
	if (rows <= 0) {
	    return true;
	}
	long skip = 0;
	while (skip < rows && rs.next() != null) {
	    skip++;
	}
	return skip == rows;
    }
}
