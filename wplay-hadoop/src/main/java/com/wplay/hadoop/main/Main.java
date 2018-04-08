package com.wplay.hadoop.main;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class Main {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage : Main <class> args...");
			return;
		}
		Configuration conf = XmlConfigUtil.create();
		int firstArg = 0;
		String clazz = args[(firstArg++)];
		Class<?> cl = Class.forName(clazz, true, Main.class.getClassLoader());
		Tool tool = (Tool) cl.newInstance();
		String[] newArgs = (String[]) Arrays.asList(args).subList(firstArg, args.length).toArray(new String[0]);
		ToolRunner.run(conf, tool, newArgs);
	}
}