package com.wplay.hadoop.job.join;

import com.wplay.hadoop.AbstractTool;
import com.wplay.hadoop.job.join.vo.Key;
import com.wplay.hadoop.job.join.vo.KeyComparor;
import com.wplay.hadoop.job.join.vo.Value;
import com.wplay.hadoop.mapreduce.output.LineOutputFormat;
import com.wplay.core.util.EnCodeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
public abstract class AbstractJoin extends AbstractTool {
	private static final String OUTPUT_NAME = "SKYOUTPUT";
	private static final String DEF_ENCODE = "UTF-8";

	private static final String T_1SPLIT = "sky.t1.split";
	private static final String T_1FIELD = "sky.t1.field";
	private static final String T_1CODE = "sky.t1.code";
	private static final String T_1PATH_CONTAINS = "sky.t1.path.contains";


	private static final String T_2SPLIT = "sky.t2.split";
	private static final String T_2FIELD = "sky.t2.field";
	private static final String T_2CODE = "sky.t2.code";

	private static final String EXIST_PREFIX = "sky.exist.prefix";
	private static final String NO_EXIST_PREFIX = "sky.exist.no.prefix";

	@Override
	protected void doAction(Path[] in, Path out) throws Exception {

		Configuration conf = this.getJobConf();
		conf.set(T_1SPLIT, t_1Split());// 表1的分隔符
		conf.setInt(T_1FIELD, t_1Field());// 表1的取那个字段
		conf.set(T_1PATH_CONTAINS, t_1PathContains());// 表的路径包含那个字符
		conf.set(T_1CODE, t_1Code());

		conf.set(T_2SPLIT, t_2Split());// 表2的分隔符
		conf.setInt(T_2FIELD, t_2Field());// 表2的取那个字段
		conf.set(T_2CODE, t_2Code());

		conf.set(EXIST_PREFIX, existPrefix());// 存在的前缀
		conf.set(NO_EXIST_PREFIX, noexistPrefix());// 不存在的前缀

		MultipleOutputs.addNamedOutput(job, OUTPUT_NAME,
				LineOutputFormat.class, Key.class, Text.class);
		this.addInputPath(in);
		this.setOutputPath(out);

		this.setMapperClass(MyMapper.class);

		this.setReducerClass(MyReducer.class);
		this.setGroupingComparatorClass(KeyComparor.class);

		this.setMapOutputKeyClass(Key.class);
		this.setMapOutputValueClass(Value.class);

		this.setOutputKeyClass(Key.class);
		this.setOutputValueClass(Text.class);

		this.setOutputFormatClass(LineOutputFormat.class);
		this.runJob(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Key, Value> {
		private Key lastKey = new Key();
		private Value lastValue = new Value();

		private Pattern t_1Pattern;
		private String t_1Split;
		private int t_1Field;
		private String t_1Code;
		private String t_1PathContains;

		private Pattern t_2Pattern;
		private String t_2Split;
		private int t_2Filed;
		private String t_2Code;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			this.t_1Split = conf.get(T_1SPLIT);
			this.t_1Field = conf.getInt(T_1FIELD, -1);
			this.t_1Code = conf.get(T_1CODE);
			this.t_1PathContains = conf.get(T_1PATH_CONTAINS);
			this.t_1Pattern = Pattern.compile(t_1Split);

			this.t_2Split = conf.get(T_2SPLIT);
			this.t_2Filed = conf.getInt(T_2FIELD, -1);
			this.t_2Code = conf.get(T_2CODE);
			this.t_2Pattern = Pattern.compile(t_2Split);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String path = ((FileSplit) context.getInputSplit()).getPath().toString();
			if (path.contains(this.t_1PathContains)) {
				String s1 = EnCodeUtil.getStr(value.getBytes(), value.getLength(), this.t_1Code);
				String vs[] = t_1Pattern.split(s1, -1);
				lastKey.setIndex(0);
				lastValue.setIndex(0);

				lastKey.setZhangh(vs[this.t_1Field].trim());
				lastValue.setValue(s1);

				context.getCounter("JOIN", "T1_NUM").increment(1l);
				context.write(lastKey, lastValue);
			} else {
				String s2 = EnCodeUtil.getStr(value.getBytes(), value.getLength(), this.t_2Code);
				String vs[] = t_2Pattern.split(s2, -1);
				lastKey.setZhangh(vs[this.t_2Filed].trim());
				lastKey.setIndex(1);
				lastValue.setIndex(1);
				lastValue.setValue(s2);

				context.getCounter("JOIN", "T2_NUM").increment(1l);
				context.write(lastKey, lastValue);
			}
		}
	}

	static class MyReducer extends Reducer<Key, Value, Key, Text> {

		private MultipleOutputs<Key, Text> mos;
		private Value first = new Value();
		private Text lastValue = new Text();
		private String existPrefix;
		private String noexistPrefix;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.mos = new MultipleOutputs<Key, Text>(context);
			Configuration conf = context.getConfiguration();
			this.existPrefix = conf.get(EXIST_PREFIX);
			this.noexistPrefix = conf.get(NO_EXIST_PREFIX);
		}

		@Override
		protected void reduce(Key key, Iterable<Value> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Value> iter = values.iterator();
			if (iter.hasNext()) {
				Value v = iter.next();
				first.setIndex(v.getIndex());
				first.setValue(v.getValue());
			}
			if (first.getIndex() == 0) {
				while (iter.hasNext()) {
					Value value = iter.next();
					if (value.getIndex() == 0) {
						continue;
					}
					lastValue.set(value.getValue());
					context.getCounter("JOIN", "EXIST_NUM").increment(1L);
					mos.write(OUTPUT_NAME, key, lastValue, this.existPrefix);
				}
			} else {
				lastValue.set(first.getValue());
				context.getCounter("JOIN", "NO_EXIST_NUM").increment(1L);
				mos.write(OUTPUT_NAME, key, lastValue, this.noexistPrefix);

				while (iter.hasNext()) {
					lastValue.set(iter.next().getValue());
					context.getCounter("JOIN", "NO_EXIST_NUM").increment(1L);
					mos.write(OUTPUT_NAME, key, lastValue, this.noexistPrefix);
				}
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if(mos != null){
				mos.close();
			}
		}
	}

	/**
	 * 表1的路径包含那个字段
	 *
	 * @return
	 */
	protected abstract String t_1PathContains();

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
	 * 表2分隔符
	 *
	 * @return
	 */
	protected abstract String t_2Split();

	/**
	 * 表1的取第几个字段
	 *
	 * @return
	 */
	protected abstract int t_1Field();

	/**
	 * 表2的取第几个字段
	 *
	 * @return
	 */
	protected abstract int t_2Field();

	/**
	 * 表二的编码
	 * @return
	 */
	protected String t_2Code(){
		return DEF_ENCODE;
	}

	/**
	 * 对于存在关联到的前缀
	 *
	 * @return
	 */
	protected abstract String existPrefix();

	/**
	 * 对于不存在关联到的前缀
	 * 
	 * @return
	 */
	protected abstract String noexistPrefix();
}
