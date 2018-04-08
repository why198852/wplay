package com.wplay.hadoop;

import com.wplay.core.util.TimingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractTool extends Configured implements Tool {
	public static final Logger LOG = Logger.getLogger(AbstractTool.class);
	protected Job job;
	protected Class<?> clazz;

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public AbstractTool() {
		this.clazz = this.getClass();
	}

	public AbstractTool(Class<?> clazz) {
		this.clazz = clazz;
	}

	public void initJob() throws IOException {
		this.job = createJob();
	}

	public void initJob(String jobName) throws IOException {
		this.job = createJob(jobName);
	}

	protected void addTmpJars(Job job) throws IOException {
		FileSystem localFs = FileSystem.getLocal(this.getConf());
		Set<String> jars = new HashSet<String>();
		jars.addAll(job.getConfiguration().getStringCollection("tmpjars"));
		String userDir = System.getProperty("user.dir");
		File libDir = new File(userDir, "lib");

		if (libDir.exists()) {
			File files[] = libDir.listFiles();
			for (File file : files) {
				Path path = new Path(file.getAbsolutePath());
				jars.add(path.makeQualified(localFs.getUri(),
						localFs.getWorkingDirectory()).toString());
			}
		}

		File commandDir = new File(userDir, "command");
		if (commandDir.exists()) {
			File files[] = commandDir.listFiles();
			for (File file : files) {
				Path path = new Path(file.getAbsolutePath());
				jars.add(path.makeQualified(localFs.getUri(),
						localFs.getWorkingDirectory()).toString());
			}
		}

		if (jars.isEmpty())
			return;
		localFs.close();
		job.getConfiguration().set(
				"tmpjars",
				StringUtils
						.arrayToString(jars.toArray(new String[jars.size()])));
	}

	protected void initJob(String jobName, Class<? extends Mapper> mapClass,
			Class<? extends Reducer> reduceClass) throws IOException {
		this.job = createJob(jobName);
		setMapperClass(mapClass);
		setReducerClass(reduceClass);
	}

	protected void initJob(String jobName, Path[] in, Path out)
			throws IOException {
		this.job = createJob(jobName);
		addInputPath(in);
		setOutputPath(out);
	}

	protected void runJob(boolean wait) throws Exception {
		this.job.waitForCompletion(wait);
	}

	private Job createJob() throws IOException {
		this.job = new Job(this.getConf());
		setJarByClass(this.clazz);
		addTmpJars(job);
		return this.job;
	}

	protected Configuration getJobConf() {
		return this.job.getConfiguration();
	}

	private Job createJob(String name) throws IOException {
		this.job = createJob();
		setJobName(name);
		return this.job;
	}

	protected void setInputFormatClass(Class<? extends InputFormat> cls) {
		this.job.setInputFormatClass(cls);
	}

	protected void setJobName(String name) {
		this.job.setJobName(name);

	}

	protected void setGroupingComparatorClass(Class<? extends RawComparator> cls) {
		this.job.setGroupingComparatorClass(cls);
	}

	protected void setJarByClass(Class<?> cls) {
		this.job.setJarByClass(cls);
	}

	protected void setNumReduceTasks(int num) {
		this.job.setNumReduceTasks(num);
	}

	protected void setMapperClass(Class<? extends Mapper> mapClass) {
		this.job.setMapperClass(mapClass);
	}

	protected void setReducerClass(Class<? extends Reducer> reduceClass) {
		this.job.setReducerClass(reduceClass);
	}

	protected void setCombinerClass(Class<? extends Reducer> reduceClass) {
		this.job.setCombinerClass(reduceClass);
	}

	protected void setOutputFormatClass(Class<? extends OutputFormat> cls) {
		this.job.setOutputFormatClass(cls);
	}

	protected void setOutputPath(Path path) {
		FileOutputFormat.setOutputPath(this.job, path);
	}

	protected void setTableName(String tableName) {
		this.getJobConf().set(TableOutputFormat.OUTPUT_TABLE, tableName);
	}

	protected void setTableOutputFormat() {
		this.setOutputFormatClass(TableOutputFormat.class);
	}

	protected void setOutputValueClass(Class<?> theClass) {
		this.job.setOutputValueClass(theClass);
	}

	protected void setMapOutputKeyClass(Class<?> theClass) {
		this.job.setMapOutputKeyClass(theClass);
	}

	protected void setMapOutputValueClass(Class<?> theClass) {
		this.job.setMapOutputValueClass(theClass);
	}

	protected void setOutputKeyClass(Class<?> theClass) {
		this.job.setOutputKeyClass(theClass);
	}

	protected void addInputPath(Path path) throws IOException {
		FileInputFormat.addInputPath(this.job, path);
	}

	protected void addInputPath(Path[] path) throws IOException {
		for (Path p : path)
			FileInputFormat.addInputPath(this.job, p);
	}

	public Class<?> getClazz() {
		return this.clazz;
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}

	protected int doAction(String[] args) throws Exception {
		String className = this.clazz.getSimpleName();
		String useAge = "Usage: " + className
				+ " <inPath1> <inPath2> ... <destPath>";
		if (args.length < 2) {
			System.err.println(useAge);
			return -1;
		}
		HashSet<Path> dirs = new HashSet<Path>();
		for (int i = 0; i < args.length - 1; i++) {
			dirs.add(new Path(args[i]));
		}
		Path destPath = new Path(args[(args.length - 1)]);
		if (this.job == null) {
			this.initJob(className + "-" + destPath);
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (Path p : dirs) {
			LOG.info("Add in path : " + p);
		}
		LOG.info("Set out path : " + destPath);
		long start = System.currentTimeMillis();
		LOG.info(className + " : starting at "
				+ sdf.format(Long.valueOf(start)));
		doAction((Path[]) dirs.toArray(new Path[dirs.size()]), destPath);
		long end = System.currentTimeMillis();
		LOG.info(className + " : finished at " + sdf.format(Long.valueOf(end))
				+ ", elapsed: " + TimingUtil.elapsedTime(start, end));
		return 0;
	}

	protected void doAction(Path[] in, Path out) throws Exception {
	}

	public Counters getCounters() throws IOException {
		return this.job.getCounters();
	}

	/**
	 * 
	 * @return
	 */
	public String getJobName() {
		return this.job.getJobName();
	}

	/**
	 * 
	 * @return
	 */
	public String getJobID() {
		return this.job.getJobID().toString();
	}

	public int run(String[] args) throws Exception {
		return doAction(args);
	}
}