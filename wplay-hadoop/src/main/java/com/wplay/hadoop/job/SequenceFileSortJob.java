package com.wplay.hadoop.job;

import com.wplay.hadoop.AbstractTool;
import com.wplay.hadoop.job.mapreduce.SequenceFileSortJobMapper;
import com.wplay.hadoop.job.mapreduce.SequenceFileSortJobReducer;
import com.wplay.hadoop.mapreduce.output.VKLineOutputFormat;
import com.wplay.hadoop.vo.DescLongWritable;
import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * SequenceFile ÎÄ¼þÅÅÐò
 * @author James
 *
 */
public class SequenceFileSortJob extends AbstractTool{

	public static void main(String args[]) throws Exception{
		Configuration conf = XmlConfigUtil.create();
		ToolRunner.run(conf, new SequenceFileSortJob(), args);
	}
	
	@Override
	public void doAction(Path[] in, Path out) throws Exception {
		this.addInputPath(in);
		this.setOutputPath(out);
		this.setInputFormatClass(SequenceFileInputFormat.class);
		this.setMapperClass(SequenceFileSortJobMapper.class);
		this.setReducerClass(SequenceFileSortJobReducer.class);
	    this.setOutputKeyClass(DescLongWritable.class);
	    this.setOutputValueClass(Text.class);
	    this.setOutputFormatClass(VKLineOutputFormat.class);
	    this.runJob(true);
	}
}
