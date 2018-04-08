package com.wplay.hbase.export;

import com.wplay.hbase.vo.HbExportVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbImport extends Configured {
	private HBaseAdmin hbaseAdmin;
	private FileSystem fs;

	public HbImport(Configuration conf) throws IOException {
		super(conf);
		this.fs = FileSystem.get(conf);
		this.hbaseAdmin = new HBaseAdmin(conf);
	}

	public void doImport(Path path) throws Exception {
		FileStatus[] fStatus = this.fs.listStatus(path, new PathFilter() {
			public boolean accept(Path path) {
				return path.getName().endsWith(".file");
			}
		});
		for (FileStatus fStatu : fStatus) {
			Path inFile = fStatu.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(this.fs,
					inFile, getConf());
			Text key = new Text();
			HbExportVO hbe = new HbExportVO();
			boolean hasNext = reader.next(key);
			while (hasNext) {
				reader.getCurrentValue(hbe);
				HTableDescriptor td = new HTableDescriptor(Bytes.toBytes(hbe
						.getTableName()));
				for (String family : hbe) {
					td.addFamily(new HColumnDescriptor(family));
				}
				this.hbaseAdmin.createTable(td);
				Path dataPath = new Path(path, hbe.getTableName());
				if (this.fs.exists(dataPath)) {
					Job job = Import.createSubmittableJob(
							getConf(),
							new String[] { hbe.getTableName(),
									dataPath.toString() });
					job.waitForCompletion(true);
				}
				hasNext = reader.next(key);
			}
			reader.close();
		}
	}
}