package com.wplay.hbase.export;

import com.wplay.hbase.mapper.WplayExporter;
import com.wplay.hbase.vo.HbExportVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Collection;

public class HbExport extends Configured {
	private HBaseAdmin hbaseAdmin;
	private FileSystem fs;

	public HbExport(Configuration conf) throws IOException {
		super(conf);
		this.fs = FileSystem.get(conf);
		this.hbaseAdmin = new HBaseAdmin(conf);
	}

	public void doExport(Path outPath, long num) throws Exception {
		this.fs.mkdirs(outPath);
		HTableDescriptor[] hds = this.hbaseAdmin.listTables();
		for (HTableDescriptor hd : hds) {
			HbExportVO heo = new HbExportVO();
			heo.setTableName(Bytes.toString(hd.getName()));
			Collection<HColumnDescriptor> hchs = hd.getFamilies();
			for (HColumnDescriptor hcd : hchs) {
				heo.addFamily(Bytes.toString(hcd.getName()));
			}
			Path outFile = new Path(outPath, heo.getTableName() + ".file");
			SequenceFile.Writer sw = SequenceFile.createWriter(this.fs,
					getConf(), outFile, Text.class, HbExportVO.class);
			sw.append(new Text(heo.getTableName()), heo);
			sw.close();
			if (num != 0L) {
				Job job = Export.createSubmittableJob(getConf(),new String[] {heo.getTableName(),new Path(outPath, heo.getTableName()).toString()});
				if (num > 0L) {
					job.setMapperClass(WplayExporter.class);
					job.getConfiguration().setLong("sky.hbase.export.rownum",num);
				}
				job.waitForCompletion(true);
			}
		}
	}

	public void doExport(Path outPath, String tableName, long num)
			throws Exception {
		this.fs.mkdirs(outPath);
		HTableDescriptor hd = this.hbaseAdmin.getTableDescriptor(Bytes
				.toBytes(tableName));
		HbExportVO heo = new HbExportVO();
		heo.setTableName(Bytes.toString(hd.getName()));
		Collection<HColumnDescriptor> hchs = hd.getFamilies();
		for (HColumnDescriptor hcd : hchs) {
			heo.addFamily(Bytes.toString(hcd.getName()));
		}
		Path outFile = new Path(outPath, tableName + ".file");
		SequenceFile.Writer sw = SequenceFile.createWriter(this.fs, getConf(),
				outFile, Text.class, HbExportVO.class);
		sw.append(new Text(heo.getTableName()), heo);
		sw.close();
		Job job = Export.createSubmittableJob(getConf(), new String[] {tableName, new Path(outPath, tableName).toString() });
		if (num > 0L) {
			job.setMapperClass(WplayExporter.class);
			job.getConfiguration().setLong("sky.hbase.export.rownum", num);
		}
		job.waitForCompletion(true);
	}
}