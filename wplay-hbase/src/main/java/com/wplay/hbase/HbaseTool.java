package com.wplay.hbase;

import com.wplay.hadoop.AbstractTool;
import com.wplay.core.util.TimingUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;

/**
 * 
 * @author James
 * 
 */
public class HbaseTool extends AbstractTool {

    @Override
    protected int doAction(String[] args) throws Exception {
	String className = this.clazz.getSimpleName();
	String useAge = "Usage: " + className + " <inPath1> <inPath2> ... <tableName>";
	if (args.length < 2) {
	    System.err.println(useAge);
	    return -1;
	}
	HashSet<Path> dirs = new HashSet<Path>();
	for (int i = 0; i < args.length - 1; i++) {
	    dirs.add(new Path(args[i]));
	}
	String tableName = args[args.length - 1];
	initJob();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	long start = System.currentTimeMillis();
	LOG.info(className + " : starting at " + sdf.format(Long.valueOf(start)));

	this.init(tableName);
	this.initOutput();
	doAction((Path[]) dirs.toArray(new Path[dirs.size()]));

	long end = System.currentTimeMillis();
	LOG.info(className + " : finished at " + sdf.format(Long.valueOf(end))
		+ ", elapsed: " + TimingUtil.elapsedTime(start, end));
	return 0;
    }

    /**
	 * 
	 */
    protected void initOutput() {
	this.setOutputKeyClass(ImmutableBytesWritable.class);
	this.setOutputValueClass(Put.class);
    }

    /**
     * 
     * @param tableName
     * @throws IOException
     */
    protected void init(String tableName) throws IOException {
	TableMapReduceUtil.initTableReducerJob(tableName, null, job,HRegionPartitioner.class);
	 this.getJobConf().set(TableOutputFormat.OUTPUT_TABLE, tableName);
	 this.setOutputFormatClass(TableOutputFormat.class);
    }

    /**
     * 
     * @param in
     */
    protected void doAction(Path[] in) throws Exception {

    }
}
