package com.wplay.hadoop.job.quchong;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * ШЅжи
 * @author James
 *
 */
public class QuChong extends AbstractQuchong{

	private String t_1Split;
	private String t_1Field;
	private String t_1Code;
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(XmlConfigUtil.create(), new QuChong(), args);
	}
	
	public int run(String[] args) throws Exception {
		CommandLine commands = printUsage(buildOptions(),args);
		this.t_1Split = commands.getOptionValue("t1split");
		this.t_1Field = commands.getOptionValue("t1field");
		this.t_1Code = commands.getOptionValue("t1code");
		
		String input = commands.getOptionValue("input");
		String output = commands.getOptionValue("output");
		String inputs[] = input.split(",");
		String parseArgs[] = new String[inputs.length + 1];
		System.arraycopy(inputs, 0, parseArgs, 0, inputs.length);
		parseArgs[inputs.length] = output;
		return doAction(parseArgs);
	}
	

	
	private static Options buildOptions() {
		Options options = new Options();
		options.addOption("t1split", true, "Table 1 data split");
		options.addOption("t1field", true, "Table 1 field");
		options.addOption("t1code", true, "Table 1 code");
		
		options.addOption("input", true, "Input directory of data,split with ','");
		options.addOption("output", true, "Output directory of data");
		return options;
	}

	@Override
	protected String t_1Split() {
		return t_1Split;
	}

	@Override
	protected String t_1Fields() {
		return t_1Field;
	}
	
	@Override
	protected String t_1Code() {
		return t_1Code;
	}
}
