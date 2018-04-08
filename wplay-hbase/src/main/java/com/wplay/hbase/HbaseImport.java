package com.wplay.hbase;

import com.wplay.hbase.solr.DataDesc;
import com.wplay.hbase.solr.DataUtil;
import com.wplay.hbase.mapper.ImporterMapper;
import com.wplay.core.util.Constants;
import com.wplay.core.util.XmlConfigUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * hbase 生成Hfile导入工具
 * @author James
 *
 */
public class HbaseImport extends AbstractJob {

    @Override
    public Options buildOptions() {
	Options options = new Options();
	options.addOption("input", true, "[must] input directory of data");
	options.addOption("output", true, "[must] output directory of data");
	options.addOption("table", true, "[must] table");
	return options;
    }

    @Override
    public boolean checkMustOption(CommandLine commands) {
	if (!(commands.hasOption("input"))) {
	    printUsage();
	    LOG.info("please set input path ");
	    return false;
	}
	if (!(commands.hasOption("output"))) {
	    printUsage();
	    LOG.info("please set output path ");
	    return false;
	}
	if (!(commands.hasOption("table"))) {
	    printUsage();
	    LOG.info("please set table");
	    return false;
	}
	return true;
    }

    @Override
    public Job createSubmittableJob(CommandLine commands, Configuration conf)
	    throws IOException {
	String commaSeparatedPaths = commands.getOptionValue(Constants.COMMAND_MR_INPUT);
	Path outputPath = new Path(commands.getOptionValue(Constants.COMMAND_MR_OUTPUT));
	// 创建job
	Job job = new Job(conf);
	job.setJobName(getJobClassName());
	job.setMapperClass(ImporterMapper.class);
	LOG.info("Input paths : " + commaSeparatedPaths);
	FileInputFormat.setInputPaths(job, commaSeparatedPaths);
	FileOutputFormat.setOutputPath(job, outputPath);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(HFileOutputFormat.class);
	job.setJarByClass(this.getClass());
	return job;
    }

    @Override
    public String getJobClassName() {
	return this.getClass().getSimpleName();
    }

    public static void main(String[] args) {
	try {
	    int re = ToolRunner.run(XmlConfigUtil.create(),new HbaseImport(), args);
	    if(re == 0){
		LOG.info("Import success!");
	    }else{
		LOG.info("Import error!");
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    LOG.error(e.getMessage(), e);
	}
    }

    @Override
    public HTable parseTableAndData(CommandLine commands,Configuration jobConf) throws IOException {
	
	String tableName = commands.getOptionValue(Constants.COMMAND_MR_TABLE);
	DataDesc datadesc = DataUtil.getTableName(tableName);
	if(datadesc == null){
	    throw new IOException("Table " + tableName + " not has data support!");
	}
	jobConf.set(Constants.COMMAND_MR_TABLE, tableName);
	jobConf.set(Constants.MR_DATA_CONF, datadesc.toBase64());
	return new HTable(this.getConf(), tableName);
    }
}
