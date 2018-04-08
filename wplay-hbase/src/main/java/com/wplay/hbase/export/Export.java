package com.wplay.hbase.export;

import com.wplay.hbase.mapper.WplayExporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Export {
	private static final Log LOG = LogFactory.getLog(Export.class);
	static final String NAME = "export";

	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {
		String tableName = args[0];
		Path outputDir = new Path(args[1]);
		Job job = new Job(conf, "export_" + tableName);
		job.setJobName("export_" + tableName);
		job.setJarByClass(WplayExporter.class);
		Scan s = getConfiguredScanForJob(conf, args);
		TableMapReduceUtil.initTableMapperJob(tableName, s, WplayExporter.class,null, null, job);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Result.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		return job;
	}

	private static Scan getConfiguredScanForJob(Configuration conf,
			String[] args) throws IOException {
		Scan s = new Scan();

		int versions = args.length > 2 ? Integer.parseInt(args[2]) : 1;
		s.setMaxVersions(versions);

		long startTime = args.length > 3 ? Long.parseLong(args[3]) : 0L;
		long endTime = args.length > 4 ? Long.parseLong(args[4])
				: 9223372036854775807L;
		s.setTimeRange(startTime, endTime);

		s.setCacheBlocks(false);

		if (conf.get("hbase.mapreduce.scan.column.family") != null) {
			s.addFamily(Bytes.toBytes(conf
					.get("hbase.mapreduce.scan.column.family")));
		}

		Filter exportFilter = getExportFilter(args);
		if (exportFilter != null) {
			LOG.info("Setting Scan Filter for Export.");
			s.setFilter(exportFilter);
		}
		LOG.info("verisons=" + versions + ", starttime=" + startTime
				+ ", endtime=" + endTime);

		return s;
	}

	private static Filter getExportFilter(String[] args) {
		Filter exportFilter = null;
		String filterCriteria = args.length > 5 ? args[5] : null;
		if (filterCriteria == null)
			return null;
		if (filterCriteria.startsWith("^")) {
			String regexPattern = filterCriteria.substring(1,
					filterCriteria.length());
			exportFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new RegexStringComparator(regexPattern));
		} else {
			exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
		}
		return exportFilter;
	}

	private static void usage(String errorMsg) {
		if ((errorMsg != null) && (errorMsg.length() > 0)) {
			System.err.println("ERROR: " + errorMsg);
		}
		System.err
				.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> [<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");

		System.err
				.println("  Note: -D properties will be applied to the conf used. ");
		System.err.println("  For example: ");
		System.err.println("   -D mapred.output.compress=true");
		System.err
				.println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
		System.err.println("   -D mapred.output.compression.type=BLOCK");
		System.err
				.println("  Additionally, the following SCAN properties can be specified");
		System.err.println("  to control/limit what is exported..");
		System.err
				.println("   -D hbase.mapreduce.scan.column.family=<familyName>");
	}
}