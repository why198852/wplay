package com.wplay.hbase.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class WplayExporter extends TableMapper<ImmutableBytesWritable, Result> {
	public static final long DEF_ROW_NUM = -1L;
	public static final String DEF_ROW_NUM_KEY = "sky.hbase.export.rownum";
	private long rowNum;
	private long currentNum = 0L;

	public void map(ImmutableBytesWritable row,Result value,Context context)
			throws IOException {
		try {
			if ((this.rowNum <= 0L) || (this.currentNum < this.rowNum)) {
				context.write(row, value);
			}
			this.currentNum += 1L;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected void setup(Context context)
			throws IOException, InterruptedException {
		this.rowNum = context.getConfiguration().getLong("sky.hbase.export.rownum", -1L);
	}
}