package com.wplay.hbase;

import com.wplay.hadoop.AbstractTool;
import com.wplay.hbase.util.HbaseUtil;
import com.wplay.core.util.TimingUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class HbaseImportTool extends AbstractTool{

	@Override
	protected int doAction(String[] args) throws Exception {
		String className = this.clazz.getSimpleName();
		String useAge = "Usage: " + className + "<tableIn> <tableOut>";
		if (args.length < 2) {
			System.err.println(useAge);
			return -1;
		}
		String tableIn = args[0];
		String tableOut = args[1];
		initJob();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		LOG.info(className + " : starting at " + sdf.format(Long.valueOf(start)));

		this.init(tableIn,tableOut);
		this.initKey();
		this.doAction();
		long end = System.currentTimeMillis();
		LOG.info(className + " : finished at " + sdf.format(Long.valueOf(end)) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
		return 0;
	}
	
	/**
	 * @param tableIn
	 * @param tableOut
	 */
	private void init(String tableIn,String tableOut){
		this.getJobConf().set(TableInputFormat.INPUT_TABLE, tableIn);
		this.setInputFormatClass(TableInputFormat.class);
		
		this.getJobConf().set(TableOutputFormat.OUTPUT_TABLE, tableOut);
		this.setOutputFormatClass(TableOutputFormat.class);
	}
	
	/**
	 * 
	 */
	private void initKey(){
		this.setOutputKeyClass(ImmutableBytesWritable.class);
	}
	
	/**
	 * 
	 * @param scan
	 * @throws IOException
	 */
	protected void registScan(Scan scan) throws IOException{
		this.getJobConf().set(TableInputFormat.SCAN, HbaseUtil.scanToString(scan));
	}
	
	/**
	 *
	 */
	protected void doAction()  throws Exception{
		
	}
}
