package com.wplay.hbase.verify;

import com.wplay.hbase.util.WriteableUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author James
 * 
 */
public class VerifyTable {

    private static final Log LOG = LogFactory.getLog(VerifyTable.class);

    public final static String NAME = "verifyTable";
    static long startTime = 0;
    static long endTime = 0;
    static String srcTable = null;
    static String families = null;
    static String targetTable = null;
    static String targetConf = null;

    /**
     * Map-only comparator for 2 tables
     */
    public static class Verifier extends
	    TableMapper<ImmutableBytesWritable, Put> {

	public static enum Counters {
	    GOODROWS, BADROWS
	}

	private ResultScanner targetScanner;

	/**
	 * Map method that compares every scanned row with the equivalent from a
	 * distant cluster.
	 * 
	 * @param row
	 *            The current table row key.
	 * @param value
	 *            The columns.
	 * @param context
	 *            The current context.
	 * @throws IOException
	 *             When something is broken with the data.
	 */
	@Override
	public void map(ImmutableBytesWritable row, final Result value,
		Context context) throws IOException {
	    if (targetScanner == null) {
		Configuration conf = context.getConfiguration();
		final Scan scan = new Scan();
		scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
		long startTime = conf.getLong(NAME + ".startTime", 0);
		long endTime = conf.getLong(NAME + ".endTime", 0);
		String families = conf.get(NAME + ".families", null);
		if (families != null) {
		    String[] fams = families.split(",");
		    for (String fam : fams) {
			scan.addFamily(Bytes.toBytes(fam));
		    }
		}
		if (startTime != 0) {
		    scan.setTimeRange(startTime,
			    endTime == 0 ? HConstants.LATEST_TIMESTAMP
				    : endTime);
		}
		HConnectionManager.execute(new HConnectable<Void>(conf) {

		    private HTable tarTable;
		    @Override
		    public Void connect(HConnection conn) throws IOException {
			Configuration tarConf = new Configuration();
			String tarConfStr = conf.get(NAME + ".targetconf");
			if (tarConfStr != null) {
			    WriteableUtil.readString(tarConf, tarConfStr);
			}else{
			    tarConf = conf;
			}
			tarTable = new HTable(tarConf, conf.get(NAME + ".targetTable"));
			scan.setStartRow(value.getRow());
			targetScanner = tarTable.getScanner(scan);
			return null;
		    }
		});
	    }
	    Result res = targetScanner.next();
	    try {
		Result.compareResults(value, res);
		context.getCounter(Counters.GOODROWS).increment(1);
	    } catch (Exception e) {
		LOG.warn("Bad row", e);
		context.getCounter(Counters.BADROWS).increment(1);
	    }
	}

	protected void cleanup(Context context) {
	    if(targetScanner != null){
		targetScanner.close();
	    }
	}
    }

    /**
     * Sets up the actual job.
     * 
     * @param conf
     *            The current configuration.
     * @param args
     *            The command line parameters.
     * @return The newly created job.
     * @throws IOException
     *             When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args)
	    throws IOException {
	if (!doCommandLine(args)) {
	    return null;
	}

	conf.set(NAME + ".srcTable", srcTable);
	conf.set(NAME + ".targetTable", targetTable);
	conf.setLong(NAME + ".startTime", startTime);
	conf.setLong(NAME + ".endTime", endTime);
	if (families != null) {
	    conf.set(NAME + ".families", families);
	}
	if(targetConf != null){
	    Configuration tarConf = new Configuration();
	    tarConf.addResource(new FileInputStream(targetConf));
	    conf.set(NAME + ".targetconf", WriteableUtil.toString(tarConf));
	}
	Job job = new Job(conf, NAME + "_" + srcTable);
	job.setJarByClass(VerifyTable.class);

	Scan scan = new Scan();
	System.out.println("starttime = " + startTime);
	if (startTime != 0) {
	    scan.setTimeRange(startTime,
		    endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
	}
	if (families != null) {
	    String[] fams = families.split(",");
	    for (String fam : fams) {
		scan.addFamily(Bytes.toBytes(fam));
	    }
	}
	TableMapReduceUtil.initTableMapperJob(srcTable, scan, Verifier.class,
		null, null, job);
	job.setOutputFormatClass(NullOutputFormat.class);
	job.setNumReduceTasks(0);
	return job;
    }

    private static boolean doCommandLine(final String[] args) {
	if (args.length < 2) {
	    printUsage(null);
	    return false;
	}
	try {
	    for (int i = 0; i < args.length; i++) {
		String cmd = args[i];
		if (cmd.equals("-h") || cmd.startsWith("--h")) {
		    printUsage(null);
		    return false;
		}

		final String startTimeArgKey = "--starttime=";
		if (cmd.startsWith(startTimeArgKey)) {
		    startTime = Long.parseLong(cmd.substring(startTimeArgKey
			    .length()));
		    continue;
		}

		final String endTimeArgKey = "--endtime=";
		if (cmd.startsWith(endTimeArgKey)) {
		    endTime = Long.parseLong(cmd.substring(endTimeArgKey
			    .length()));
		    continue;
		}

		final String familiesArgKey = "--families=";
		if (cmd.startsWith(familiesArgKey)) {
		    families = cmd.substring(familiesArgKey.length());
		    continue;
		}
		
		final String targetConfArgKey = "--targetConf";
		if(cmd.startsWith(targetConfArgKey)){
		    targetConf = cmd.substring(targetConfArgKey.length());
		    continue;
		}

		if (i == args.length - 2) {
		    srcTable = cmd;
		}

		if (i == args.length - 1) {
		    targetTable = cmd;
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    printUsage("Can't start because " + e.getMessage());
	    return false;
	}
	return true;
    }

    /*
     * @param errorMsg Error message. Can be null.
     */
    private static void printUsage(final String errorMsg) {
	if (errorMsg != null && errorMsg.length() > 0) {
	    System.err.println("ERROR: " + errorMsg);
	}
	System.err.println("Usage: verifytable [--starttime=X]"
		+ " [--stoptime=Y] [--families=A] [--targetConf=file] <srcTable> <targetTable>");
	System.err.println();
	System.err.println("Options:");
	System.err.println(" starttime    beginning of the time range");
	System.err
		.println("              without endtime means from starttime to forever");
	System.err.println(" stoptime     end of the time range");
	System.err.println(" families     comma-separated list of families to copy");
	System.err.println(" targetConf   Target config file");
	System.err.println();
	System.err.println("Args:");
	System.err
		.println(" srcTable       Name of the original table to verify");
	System.err.println(" targetTable    Name of the target table to verify");
	System.err.println();
	System.err.println("Examples:");
	System.err
		.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
	System.err.println(" $ bin/hbase "
			+ "com.skycloud.hbase.verify.VerifyTable"
			+ " --starttime=1265875194289 --stoptime=1265878794289 TestTable1 TestTable2 ");
    }

    /**
     * Main entry point.
     * 
     * @param args
     *            The command line parameters.
     * @throws Exception
     *             When running the job fails.
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	Job job = createSubmittableJob(conf, args);
	if (job != null) {
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    }

}
