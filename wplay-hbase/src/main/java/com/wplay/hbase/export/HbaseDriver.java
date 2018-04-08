package com.wplay.hbase.export;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HbaseDriver {
	public static final String FILE_SUFFIX = ".file";

	public static void main(String[] args) throws Exception {
		String useAge = "<-import|-export [[-tn <tableName>] [-num <num>]]> <path>";
		if (args.length < 2) {
			System.err.println(useAge);
			return;
		}
		String opt = args[0];
		Configuration conf = XmlConfigUtil.create();
		if (opt.equals("-import")) {
			String path = args[1];
			HbImport hbImport = new HbImport(conf);
			hbImport.doImport(new Path(path));
		} else if (opt.equals("-export")) {
			String exOpt = args[1];
			HbExport hbExport = new HbExport(conf);
			if (exOpt.equals("-tn")) {
				String tableName = args[2];
				String option = args[3];
				if (option.equals("-num")) {
					long num = Long.parseLong(args[4]);
					String file = args[5];
					hbExport.doExport(new Path(file), tableName, num);
				} else {
					String file = args[3];
					hbExport.doExport(new Path(file), tableName, -1L);
				}
			} else if (exOpt.equals("-num")) {
				long num = Long.parseLong(args[2]);
				String file = args[3];
				hbExport.doExport(new Path(file), num);
			} else {
				hbExport.doExport(new Path(exOpt), -1L);
			}
		} else {
			System.err.println(useAge);
			return;
		}
	}
}

/*
 * Location: /opt/brainbook/sky-core/sky-core-1.0.jar Qualified Name:
 * com.sky.hbase.export.HbaseDriver JD-Core Version: 0.6.0
 */