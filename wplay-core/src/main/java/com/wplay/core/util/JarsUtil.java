package com.wplay.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @ClassName 
 * @Description 
 * @author James
 * @date
 */
public class JarsUtil {

	public static void addTmpJars(Configuration conf) {
		try {
			FileSystem localFs = FileSystem.getLocal(conf);
			Set<String> jars = new HashSet<String>();
			jars.addAll(conf.getStringCollection("tmpjars"));
			String userDir = System.getProperty("user.dir");
			File libDir = new File(userDir, "lib");

			if (libDir.exists()) {
				File files[] = libDir.listFiles();
				for (File file : files) {
					Path path = new Path(file.getAbsolutePath());
					jars.add(path.makeQualified(localFs.getUri(),localFs.getWorkingDirectory()).toString());
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
			conf.set("tmpjars",StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
