package com.wplay.hadoop.job;

import com.wplay.hadoop.AbstractTool;
import com.wplay.hadoop.job.mapreduce.TextSortJobMapper;
import com.wplay.hadoop.job.mapreduce.TextSortJobReducer;
import com.wplay.hadoop.mapreduce.output.VKLineOutputFormat;
import com.wplay.hadoop.vo.DescLongWritable;
import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

/**
 * Text ÎÄ¼þÅÅÐò
 * @author James
 *
 */
public class TextFileSortJob extends AbstractTool{

	public static void main(String args[]) throws Exception{
		Configuration conf = XmlConfigUtil.create();
		ToolRunner.run(conf, new TextFileSortJob(), args);
	}
	
	@Override
	public void doAction(Path[] in, Path out) throws Exception {
		this.addInputPath(in);
		this.setOutputPath(out);
		this.setMapperClass(TextSortJobMapper.class);
		this.setReducerClass(TextSortJobReducer.class);
		this.setOutputKeyClass(DescLongWritable.class);
		this.setOutputValueClass(Text.class);
		this.setOutputFormatClass(VKLineOutputFormat.class);
		this.runJob(true);
	}
}
