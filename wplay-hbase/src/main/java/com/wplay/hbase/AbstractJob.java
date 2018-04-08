package com.wplay.hbase;

import com.wplay.hbase.reduce.KeyValueSortReducer;
import com.wplay.core.util.Constants;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * MR抽象类
 */
public abstract class AbstractJob extends Configured implements Tool {

    protected static final Log LOG = LogFactory.getLog(AbstractJob.class);
    static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

    private Options options = null;

    public AbstractJob() {
    }

    /**
     * 建立options
     *
     * @return
     */
    public abstract Options buildOptions();

    /**
     * 打印异常options
     */
    public void printUsage() {
	printUsage(options);
    }

    public void printUsage(Options options) {
	HelpFormatter help = new HelpFormatter();
	help.printHelp(getJobClassName(), options);
    }

    @Override
    public int run(String args[]) throws Exception {
	int exitCode = -1;

	options = buildOptions();
	CommandLine commands = null;
	try {
	    BasicParser parser = new BasicParser();
	    commands = parser.parse(options, args);
	} catch (ParseException e) {
	    printUsage(options);
	    return exitCode;
	}

	if (!checkMustOption(commands)) {
	    return exitCode;
	}

	// 使用同步创建配置项
	Configuration conf = this.getConf();

	Job job = createSubmittableJob(commands, conf); // 将配置conf传递过去

	addTmpJars(job);

	HTable table = parseTableAndData(commands, job.getConfiguration());

	job.setOutputKeyClass(ImmutableBytesWritable.class);
	job.setOutputValueClass(KeyValue.class);
	HFileOutputFormat.configureIncrementalLoad(job, table);

	job.setReducerClass(KeyValueSortReducer.class);

	MultipleOutputs.addNamedOutput(job, Constants.ERROR_FILE_OUTPUT,
		TextOutputFormat.class, NullWritable.class, Text.class);
	exitCode = (job.waitForCompletion(true)) ? 0 : 1;
	return exitCode;
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
		jars.add(path.makeQualified(localFs.getUri(),localFs.getWorkingDirectory()).toString());
	    }
	}

	if (jars.isEmpty())
	    return;
	localFs.close();
	job.getConfiguration() .set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars
				.size()])));
    }

    public abstract HTable parseTableAndData(CommandLine commands,
	    Configuration jobConf) throws IOException;

    /**
     * 检查必须参数
     *
     * @param commands
     * @return
     */
    public abstract boolean checkMustOption(CommandLine commands);

    /**
     * 创建Job
     *
     * @param commands
     * @param conf
     *            传递 conf
     * @return
     * @throws IOException
     */
    public abstract Job createSubmittableJob(CommandLine commands,
	    Configuration conf) throws IOException;

    /**
     * 获取类名字
     * 
     * @return
     */
    public abstract String getJobClassName();
}