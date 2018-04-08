package com.wplay.hbase.reader;

import com.wplay.hbase.HTableFactory;
import com.wplay.hbase.util.BaseTypeUtil;
import com.wplay.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

public abstract class AbstractReader<V> implements Reader<V> {

	public static final String METHOD_SET = "set";
	public static final String METHOD_GET = "get|is";
	private HTableFactory htableFactory;
	private Configuration conf;
	private Set<byte[]> skipRow = new HashSet<byte[]>();
	protected Class<V> vClass;
	private Filter filter;

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	protected AbstractReader(Configuration conf, Class<V> vClass) {
		this.conf = conf;
		this.htableFactory = HTableFactory.getHTableFactory(conf);
		this.vClass = vClass;
	}

	protected void addSkipRow(String rowKey) {
		skipRow.add(Bytes.toBytes(rowKey));
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

	@Override
	public List<V> goPage(String tableName, String startRow, long currentPage, long goPage, long limit) throws Exception {
		if (goPage <= 1) {
			return this.read(tableName, null, limit);
		} else {
			HTableInterface htable = htableFactory.getHTable(tableName);
			try {
				if (goPage > currentPage) {
					long pages = goPage - currentPage;
					Scan scan = new Scan();
					long rows = (pages - 1) * limit;
					if (!StringUtil.isEmpty(startRow)) {
						scan.setStartRow(Bytes.toBytes(startRow));
						rows += 1;
					}
					ResultScanner rs = htable.getScanner(scan);
					if (this.skip(rs, rows)) {
						return this.read(rs, limit);
					}
				} else {
					Scan scan = new Scan();
					ResultScanner rs = htable.getScanner(scan);
					if (this.skip(rs, limit * (goPage - 1))) {
						return this.read(rs, limit);
					}
				}
			} finally {
				htableFactory.release(htable);
			}
			return null;
		}
	}

	@Override
	public List<V> query(String tableName, String startRow, String prefix, long currentPage, long goPage, long limit) throws Exception {
		if (goPage <= 0) {
			return this.query(tableName, prefix, startRow, limit);
		} else {
			HTableInterface htable = htableFactory.getHTable(tableName);
			try {
				if (goPage > currentPage) {
					long pages = goPage - currentPage;
					Scan scan = new Scan();
					long rows = (pages - 1) * limit;
					if (!StringUtil.isEmpty(startRow)) {
						scan.setStartRow(Bytes.toBytes(startRow));
						rows += 1;
					}
					this.initPrefixFilter(scan, prefix);
					ResultScanner rs = htable.getScanner(scan);
					if (this.skip(rs, rows)) {
						return this.read(rs, limit);
					}
				} else {
					Scan scan = new Scan();
					this.initPrefixFilter(scan, prefix);
					ResultScanner rs = htable.getScanner(scan);
					if (this.skip(rs, limit * (goPage - 1))) {
						return this.read(rs, limit);
					}
				}
			} finally {
				htableFactory.release(htable);
			}
			return null;
		}
	}

	private void initPrefixFilter(Scan scan, String prefix) {
		FilterList fl = new FilterList();
		if (this.filter != null) {
			fl.addFilter(filter);
		}
		fl.addFilter(new PrefixFilter(Bytes.toBytes(prefix)));
		scan.setFilter(fl);
	}

	@Override
	public V get(String tableName, String rowKey) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = htable.get(get);
			return this.parse(result);
		} finally {
			htableFactory.release(htable);
		}
	}

	@Override
	public List<V> query(String tableName, String prefix, String startRow, long limit) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			long rows = 0;
			if (!StringUtil.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
				rows += 1;
			}
			this.initPrefixFilter(scan, prefix);
			limit = limit <= 0 ? Long.MAX_VALUE : limit;
			ResultScanner rs = htable.getScanner(scan);
			if (rs != null) {
				if (this.skip(rs, rows)) {
					return this.read(rs, limit);
				}
			}
			return null;
		} finally {
			htableFactory.release(htable);
		}
	}

	/**
	 * 
	 * @param rs
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	private List<V> read(ResultScanner rs, long limit) throws Exception {
		List<V> results = new ArrayList<V>();
		for (int i = 0; results.size() < limit; i++) {
			Result result = rs.next();
			if (result != null) {
				V v = parse(result);
				if (v != null) {
					results.add(v);
				}
			} else {
				break;
			}
		}
		return results.isEmpty() ? null : results;
	}

	/**
	 * 
	 * @param rs
	 * @return
	 * @throws Exception
	 */
	private List<V> read(ResultScanner rs) throws Exception {
		List<V> results = new ArrayList<V>();
		for (Result re : rs) {
			if (re != null) {
				V v = parse(re);
				if (v != null) {
					results.add(v);
				}
			}
		}
		return results.isEmpty() ? null : results;
	}

	@Override
	public List<V> read(String tableName, String startRow, long limit) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			long rows = 0;
			if (!StringUtil.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
				rows += 1;
			}
			if (this.filter != null) {
				scan.setFilter(filter);
			}
			limit = limit <= 0 ? Long.MAX_VALUE : limit;
			ResultScanner rs = htable.getScanner(scan);
			if (rs != null) {
				if (this.skip(rs, rows)) {
					return this.read(rs, limit);
				}
			}
			return null;
		} finally {
			htableFactory.release(htable);
		}
	}

	@Override
	public List<V> read(String tableName, String startRow, String stopRow) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			if (!StringUtil.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
			}
			if (StringUtil.isEmpty(stopRow)) {
				scan.setStopRow(Bytes.toBytes(stopRow));
			}
			if (this.filter != null) {
				scan.setFilter(filter);
			}
			ResultScanner rs = htable.getScanner(scan);
			if (rs != null) {
				return this.read(rs);
			}
			return null;
		} finally {
			htableFactory.release(htable);
		}
	}

	protected void setValue(V v, Method[] methods, String field, byte[] value) throws Exception {
		for (Method method : methods) {
			String name = toField(method.getName());
			if (name.equalsIgnoreCase(field)) {
				Class<?>[] clazzes = method.getParameterTypes();
				if (clazzes != null && clazzes.length == 1) {
					Class<?> calzz = clazzes[0];
					Object o = BaseTypeUtil.getValue(calzz, value);
					if (o != null) {
						method.invoke(v, o);
					}
				}
			}
		}
	}

	private String toField(String method) {
		method = method.replaceFirst("^" + METHOD_SET, "");
		char v = method.charAt(0);
		return method.replaceAll("^" + v, String.valueOf(Character.toLowerCase(v)));
	}

	private V parse(Result result) throws Exception {
		V v = vClass.newInstance();
		Method[] methods = vClass.getMethods();
		byte rowKey[] = result.getRow();
		if (skipRow.contains(rowKey)) {
			return null;
		}
		setValue(v, methods, "rowKey", rowKey); // …Ë÷√RowKey
		for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result.getMap().entrySet()) {
			byte family[] = entry.getKey();
			String fam = Bytes.toString(family);
			NavigableMap<byte[], NavigableMap<Long, byte[]>> value = entry.getValue();
			for (Map.Entry<byte[], NavigableMap<Long, byte[]>> chEntry : value.entrySet()) {
				byte qualifier[] = chEntry.getKey();
				NavigableMap<Long, byte[]> chValue = chEntry.getValue();
				String field = Bytes.toString(qualifier);
				setValue(v, methods, field, chValue.lastEntry().getValue());
			}
		}
		return v;
	}

	/**
	 * 
	 * @param rs
	 * @return
	 * @throws IOException
	 */
	private long getTotal(ResultScanner rs) throws IOException {
		long total = 0;
		if (rs != null) {
			Result re = rs.next();
			while (re != null) {
				total++;
				re = rs.next();
			}
		}
		return total;
	}

	@Override
	public long queryTotal(String tableName, String prefix) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			this.initPrefixFilter(scan, prefix);
			ResultScanner rs = htable.getScanner(scan);
			return this.getTotal(rs);
		} finally {
			htableFactory.release(htable);
		}
	}

	@Override
	public long getTotal(String tableName) throws IOException {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			ResultScanner rs = htable.getScanner(scan);
			return this.getTotal(rs);
		} finally {
			htableFactory.release(htable);
		}
	}

	@Override
	public List<V> goPage(String tableName, String startRow, String endRow,
			long start, long limit) throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			if (!StringUtil.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
			}
			if (StringUtil.isEmpty(endRow)) {
				scan.setStopRow(Bytes.toBytes(endRow));
			}
			if (this.filter != null) {
				scan.setFilter(filter);
			}
			ResultScanner rs = htable.getScanner(scan);
			if (rs != null) {
				this.skip(rs, start);
				return this.read(rs,limit);
			}
			return null;
		} finally {
			htableFactory.release(htable);
		}
	}

	@Override
	public long queryTotal(String tableName, String startRow, String endRow)
			throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		try {
			Scan scan = new Scan();
			if (!StringUtil.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
			}
			if (StringUtil.isEmpty(endRow)) {
				scan.setStopRow(Bytes.toBytes(endRow));
			}
			if (this.filter != null) {
				scan.setFilter(filter);
			}
			ResultScanner rs = htable.getScanner(scan);
			if (rs != null) {
				return this.getTotal(rs);
			}
			return 0;
		} finally {
			htableFactory.release(htable);
		}
	}
}
