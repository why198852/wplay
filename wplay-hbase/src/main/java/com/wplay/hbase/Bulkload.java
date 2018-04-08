package com.wplay.hbase;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

public class Bulkload extends Configured {
	private static final Logger LOG = Logger.getLogger(Bulkload.class);
	private LoadIncrementalHFiles load;

	public Bulkload(Configuration conf) throws Exception {
		super(conf);
		load = new LoadIncrementalHFiles(conf);
		Decompressor decompressor = Algorithm.GZ.getDecompressor();
		LOG.info("Init native GZ decompressor " + decompressor);
	}
	
	public void bulkload(String table,Path input) throws Exception {
		LOG.info("Will load path " + input + " to table " + table);
		HTable hTable = new HTable(getConf(),table);
//		FileSystem fs = FileSystem.get(getConf());
//		input = input.makeQualified(fs.getUri(), fs.getWorkingDirectory());
		try{
			load.doBulkLoad(input, hTable);
		}finally{
			hTable.close();
		}
		LOG.info("Load path " + input + " to table " + table + " finish");
	}
	
	public static void main(String[] args) throws Exception {
		String usage = "Usage : Bulkload <table> <path>";
		if(args.length != 2){
			System.out.println(usage);
			System.exit(-1);
		}
		String table = args[0];
		Path input = new Path(args[1]);
		Configuration conf = XmlConfigUtil.create();
		Bulkload bl = new Bulkload(conf);
		bl.bulkload(table, input);
	}
}
