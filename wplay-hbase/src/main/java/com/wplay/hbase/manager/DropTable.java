package com.wplay.hbase.manager;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class DropTable {
	public static void printUsage() {
		System.err.println("Usage : DropTable <tableName>");
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			printUsage();
			return;
		}
		String tableName = args[0];
		Configuration conf = XmlConfigUtil.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}
}