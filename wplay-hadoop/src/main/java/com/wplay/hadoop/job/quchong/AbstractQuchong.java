package com.wplay.hadoop.job.quchong;

import com.wplay.hadoop.AbstractTool;
import com.wplay.hadoop.job.join.InputJoin;
import com.wplay.hadoop.mapreduce.output.LineOutputFormat;
import com.wplay.core.util.EnCodeUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * join基础类
 *
 * @author James
 *
 */
public abstract class AbstractQuchong extends AbstractTool {
	private static final String OUTPUT_NAME = "SKYOUTPUT";
	private static final String DEF_ENCODE = "UTF-8";

	private static final String T_1SPLIT = "sky.t1.split";
	private static final String T_1FIELD = "sky.t1.field";
	private static final String T_1CODE = "sky.t1.code";
	private static final String MAX_CF = "sky.max.cf";

	public static CommandLine printUsage(Options options,String args[]) {
		HelpFormatter help = new HelpFormatter();
		CommandLine commands = null;
		try {
			BasicParser parser = new BasicParser();
			commands = parser.parse(options, args);
		} catch (ParseException e) {
			help.printHelp(InputJoin.class.getSimpleName(), options);
			System.exit(-1);
		}

		for(Object o : options.getOptions()){
			Option option = (Option)o;
			if(!commands.hasOption(option.getOpt())){
				help.printHelp(InputJoin.class.getSimpleName(), options);
				System.exit(-1);
			}
		}
		return commands;
	}

	@Override
	protected void doAction(Path[] in, Path out) throws Exception {

		Configuration conf = this.getJobConf();
		conf.set(T_1SPLIT, t_1Split());
		conf.set(T_1FIELD, t_1Fields());
		conf.set(T_1CODE, t_1Code());
		conf.setLong(MAX_CF, maxCF());

		MultipleOutputs.addNamedOutput(job, OUTPUT_NAME,LineOutputFormat.class, Text.class, Text.class);
		this.addInputPath(in);
		this.setOutputPath(out);

		this.setMapperClass(MyMapper.class);

		this.setReducerClass(MyReducer.class);

		this.setOutputKeyClass(Text.class);
		this.setOutputValueClass(Text.class);

		this.setOutputFormatClass(LineOutputFormat.class);
		this.runJob(true);
	}


	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Pattern t_1Pattern;
		private String t_1Split;
		private int[] t_1Field;
		private String t_1Code;

		private Text lastKey = new Text();
		private Text lastValue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			this.t_1Split = conf.get(T_1SPLIT);
			this.t_1Field = conf.getInts(T_1FIELD);
			this.t_1Code = conf.get(T_1CODE);
			this.t_1Pattern = Pattern.compile(t_1Split);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s1 = EnCodeUtil.getStr(value.getBytes(), value.getLength(), this.t_1Code);
			String vs[] = t_1Pattern.split(s1, -1);
			StringBuffer sb = new StringBuffer();
			for(int i : t_1Field){
				sb.append(vs[i]).append("|");
			}
			lastKey.set(sb.toString());
			lastValue.set(s1);
			context.write(lastKey, lastValue);
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private long maxCF = 1;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			long total = 0;
			while(iter.hasNext()){
				context.write(key, iter.next());
				total ++;
				if(total == maxCF){
					return;
				}
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			maxCF = context.getConfiguration().getLong(MAX_CF, 1);
		}
	}


	/**
	 * 最大重复数
	 * @return
	 */
	protected long maxCF(){
		return 1;
	}

	/**
	 * 表1分隔符
	 *
	 * @return
	 */
	protected abstract String t_1Split();

	/**
	 * 表一的编码
	 * @return
	 */
	protected String t_1Code(){
		return DEF_ENCODE;
	}

	/**
	 * 表1的取第几个字段
	 * 
	 * @return
	 */
	protected abstract String t_1Fields();
}
