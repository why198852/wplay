package com.wplay.hadoop.job.join;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * 编码的输入数据实现
 * @author James
 *
 */
public class EnCodeInputJoin extends AbstractJoin{

	private String t_1Split;
	private int t_1Field;
	private String t_1PathContains;
	private String t_1Code;
	
	private String t_2Split;
	private int t_2Filed;
	private String t_2Code;
	
	private String existPrefix;
	private String noexistPrefix;
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(XmlConfigUtil.create(), new EnCodeInputJoin(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		CommandLine commands = printUsage(args);
		this.t_1Split = commands.getOptionValue("t1split");
		this.t_1Field = Integer.parseInt(commands.getOptionValue("t1field"));
		this.t_1PathContains = commands.getOptionValue("t1pathc");
		this.t_1Code = commands.getOptionValue("t1code");
		
		this.t_2Split = commands.getOptionValue("t2split");
		this.t_2Filed = Integer.parseInt(commands.getOptionValue("t2filed"));
		this.t_2Code = commands.getOptionValue("t2code");
		
		this.existPrefix = commands.getOptionValue("exist");
		this.noexistPrefix = commands.getOptionValue("noexist");
		
		String input = commands.getOptionValue("input");
		String output = commands.getOptionValue("output");
		String inputs[] = input.split(",");
		String parseArgs[] = new String[inputs.length + 1];
		System.arraycopy(inputs, 0, parseArgs, 0, inputs.length);
		parseArgs[inputs.length] = output;
		return doAction(parseArgs);
	}
	
	public static CommandLine printUsage(String args[]) {
		HelpFormatter help = new HelpFormatter();
		Options options = buildOptions();
		CommandLine commands = null;
		try {
			BasicParser parser = new BasicParser();
			commands = parser.parse(options, args);
		} catch (ParseException e) {
			help.printHelp(EnCodeInputJoin.class.getSimpleName(), options);
			System.exit(-1);
		}
		
		for(Object o : options.getOptions()){
			Option option = (Option)o;
			if(!commands.hasOption(option.getOpt())){
				help.printHelp(EnCodeInputJoin.class.getSimpleName(), options);
				System.exit(-1);
			}
		}
		return commands;
	}
	
	private static Options buildOptions() {
		Options options = new Options();
		options.addOption("t1split", true, "Table 1 data split");
		options.addOption("t1field", true, "Table 1 field");
		options.addOption("t1pathc", true, "Table 1 path contains");
		options.addOption("t1code", true, "Table 1 encode");
		
		
		options.addOption("t2split", true, "Table 2 data split");
		options.addOption("t2filed", true, "Table 2 field");
		options.addOption("t2code", true, "Table 2 encode");
		
		options.addOption("exist", true, "Exist data output prefix");
		options.addOption("noexist", true, "Not exist data output prefix");
		
		options.addOption("input", true, "Input directory of data,split with ','");
		options.addOption("output", true, "Output directory of data");
		return options;
	}
	
	@Override
	protected String t_1PathContains() {
		return this.t_1PathContains;
	}

	@Override
	protected String t_1Split() {
		return this.t_1Split;
	}

	@Override
	protected String t_2Split() {
		return this.t_2Split;
	}

	@Override
	protected int t_1Field() {
		return this.t_1Field;
	}

	@Override
	protected int t_2Field() {
		return this.t_2Filed;
	}

	@Override
	protected String existPrefix() {
		return this.existPrefix;
	}

	@Override
	protected String noexistPrefix() {
		return this.noexistPrefix;
	}

	@Override
	protected String t_1Code() {
		return t_1Code;
	}

	@Override
	protected String t_2Code() {
		return t_2Code;
	}
}
