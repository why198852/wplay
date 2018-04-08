package com.wplay.hbase.manager;

import com.wplay.hbase.HTableFactory;
import com.wplay.core.util.XmlConfigUtil;
import com.wplay.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteRecode {
	private HTableFactory htableFactory;
	private Configuration conf;

	public DeleteRecode(Configuration conf) {
		this.conf = conf;
		this.htableFactory = HTableFactory.getHTableFactory(conf);
	}

	public void delete(String tableName, String startRow, long limit)
			throws Exception {
		HTableInterface htable = this.htableFactory.getHTable(tableName);
		Scan scan = new Scan();
		if (!StringUtil.isEmpty(startRow)) {
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		limit = limit <= 0L ? Long.MAX_VALUE : limit;
		ResultScanner rs = htable.getScanner(scan);
		if (rs != null)
			for (long i = 0L; i < limit; i += 1L) {
				Result result = rs.next();
				if (result != null) {
					Delete delete = new Delete(result.getRow());
					htable.delete(delete);
					System.out.println("delete lan = " + i);
				}
			}
	}

	public static void main(String[] args) throws Exception {
		if ((args.length < 1) || (args.length > 5)) {
			printUsage();
			return;
		}
		String tableName = args[0];
		long limit = 0L;
		String startRow = "";
		try {
			for (int i = 1; i < args.length; i += 2)
				if (args[i].equals("-start")) {
					startRow = args[(i + 1)];
				} else if (args[i].equals("-limit")) {
					limit = StringUtil.toLong(args[(i + 1)]);
				} else {
					printUsage();
					return;
				}
		} catch (Exception e) {
			printUsage();
			return;
		}
		Configuration conf = XmlConfigUtil.create();
		DeleteRecode dr = new DeleteRecode(conf);
		dr.delete(tableName, startRow, limit);
	}

	public static void printUsage() {
		System.err.println("<tableName> [-start <startRow>] [-limit <limit>]");
	}
}