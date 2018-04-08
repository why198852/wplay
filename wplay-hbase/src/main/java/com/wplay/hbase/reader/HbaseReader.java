package com.wplay.hbase.reader;

import com.wplay.hbase.HTableFactory;
import com.wplay.core.util.XmlConfigUtil;
import com.wplay.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.NavigableMap;

public class HbaseReader {

	private HTableFactory htableFactory;
	private Configuration conf;

	public HbaseReader(Configuration conf) {
		this.conf = conf;
		this.htableFactory = HTableFactory.getHTableFactory(conf);
	}

	public void read(String tableName, String startRow, long limit)
			throws Exception {
		HTableInterface htable = htableFactory.getHTable(tableName);
		Scan scan = new Scan();
		if (!StringUtil.isEmpty(startRow)) {
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		limit = limit <= 0 ? Long.MAX_VALUE : limit;
		Filter pageFilter = new PageFilter(limit);
		scan.setFilter(pageFilter);
		ResultScanner rs = htable.getScanner(scan);
		for(Result result : rs){
			System.out.println(outputResult(result));
		}
	}

	public String outputResult(Result result) {
		StringBuffer sb = new StringBuffer();
		byte rowKey[] = result.getRow();
		sb.append("'\n rowkey=").append(Bytes.toString(rowKey)).append("',\n");
		for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result.getMap().entrySet()) {
			byte family[] = entry.getKey();
			String fam = Bytes.toString(family);
			NavigableMap<byte[], NavigableMap<Long, byte[]>> value = entry
					.getValue();
			for (Map.Entry<byte[], NavigableMap<Long, byte[]>> chEntry : value
					.entrySet()) {
				byte qualifier[] = chEntry.getKey();
				NavigableMap<Long, byte[]> chValue = chEntry.getValue();
				sb.append("'column=").append(fam).append(":")
						.append(Bytes.toString(qualifier)).append("',");
				for (Map.Entry<Long, byte[]> chChEntry : chValue.entrySet()) {
//					long key = chChEntry.getKey();
//					sb.append("'timestamp=").append(key).append("',");
					byte chChValue[] = chChEntry.getValue();
					if (chChValue != null) {
						sb.append("'value=")
								.append(Bytes.toStringBinary(chChValue))
								.append("',\n");
					} else {
						sb.append("'value=',\n");
					}
				}
			}
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 1 || args.length > 5) {
			printUsage();
			return;
		}
		String tableName = args[0];
		long limit = 0;
		String startRow = "";
		try {
			for (int i = 1; i < args.length; i += 2) {
				if (args[i].equals("-start")) {
					startRow = args[i + 1];
				} else if (args[i].equals("-limit")) {
					limit = StringUtil.toLong(args[i + 1]);
				} else {
					printUsage();
					return;
				}
			}
		} catch (Exception e) {
			printUsage();
			return;
		}
		Configuration conf = XmlConfigUtil.create();
		HbaseReader hr = new HbaseReader(conf);
		hr.read(tableName, startRow, limit);
	}

	public static void printUsage() {
		System.err.println("<tableName> [-start <startRow>] [-limit <limit>]");
	}
}
