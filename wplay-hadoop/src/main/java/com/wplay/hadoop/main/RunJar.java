package com.wplay.hadoop.main;

import com.wplay.hadoop.AbstractTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.PlatformName;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class RunJar {

	public static void unJar(File jarFile, File toDir) throws IOException {
		JarFile jar = new JarFile(jarFile);
		try {
			Enumeration<JarEntry> entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = entries.nextElement();
				if (!entry.isDirectory()) {
					InputStream in = jar.getInputStream(entry);
					try {
						File file = new File(toDir, entry.getName());
						if (!file.getParentFile().mkdirs()) {
							if (!file.getParentFile().isDirectory()) {
								throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
							}
						}
						OutputStream out = new FileOutputStream(file);
						try {
							byte[] buffer = new byte[8192];
							int i;
							while ((i = in.read(buffer)) != -1) {
								out.write(buffer, 0, i);
							}
						} finally {
							out.close();
						}
					} finally {
						in.close();
					}
				}
			}
		} finally {
			jar.close();
		}
	}

	/**
	 * ${HADOOP_HOME}/lib/native/${JAVA_PLATFORM} 动态添加 JAVA_LIBRARY_PATH
	 *
	 * @param s
	 * @throws IOException
	 */
	public static void addDir(String s) throws IOException {
		try {
			Field field = ClassLoader.class.getDeclaredField("usr_paths");
			field.setAccessible(true);
			String[] paths = (String[]) field.get(null);
			for (int i = 0; i < paths.length; i++) {
				if (s.equals(paths[i])) {
					return;
				}
			}
			String[] tmp = new String[paths.length + 1];
			System.arraycopy(paths, 0, tmp, 0, paths.length);
			tmp[paths.length] = s;
			field.set(null, tmp);
		} catch (IllegalAccessException e) {
			throw new IOException("Failed to get permissions to set library path");
		} catch (NoSuchFieldException e) {
			throw new IOException("Failed to get field handle to set library path");
		}
	}

	/**
	 *
	 * @param jarName
	 * @param runClass
	 * @param args
	 * @throws Throwable
	 */
	public static void runJar(Configuration conf, String jarName,
			String runClass, String[] args) throws Throwable {
		int firstArg = 0;
		File file = new File(jarName);
		String mainClassName = null;

		JarFile jarFile;
		try {
			jarFile = new JarFile(jarName);
		} catch (IOException io) {
			throw new IOException("Error opening job jar: " + jarName)
					.initCause(io);
		}

		Manifest manifest = jarFile.getManifest();
		if (manifest != null) {
			mainClassName = manifest.getMainAttributes().getValue("Main-Class");
		}
		jarFile.close();

		if (mainClassName == null) {
			mainClassName = runClass;
		}
		mainClassName = mainClassName.replaceAll("/", ".");

		File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
		tmpDir.mkdirs();
		if (!tmpDir.isDirectory()) {
			System.err.println("Mkdirs failed to create " + tmpDir);
			System.exit(-1);
		}
		final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
		workDir.delete();
		workDir.mkdirs();
		if (!workDir.isDirectory()) {
			System.err.println("Mkdirs failed to create " + workDir);
			System.exit(-1);
		}
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					FileUtil.fullyDelete(workDir);
				} catch (Exception e) {
				}
			}
		});
		unJar(file, workDir);
		ArrayList<URL> classPath = new ArrayList<URL>();
		classPath.add(new File(workDir + "/").toURI().toURL());
		classPath.add(file.toURI().toURL());
		classPath.add(new File(workDir, "classes/").toURI().toURL());
		File[] libs = new File(workDir, "lib").listFiles();
		if (libs != null) {
			for (int i = 0; i < libs.length; i++) {
				classPath.add(libs[i].toURI().toURL());
			}
		}

		initJavaLibrayPath(conf); // 初始化java libray path
		ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));

		Thread.currentThread().setContextClassLoader(loader);
		Class<?> mainClass = Class.forName(mainClassName, true, loader);
		Method main = mainClass.getMethod("main", new Class[] { Array
				.newInstance(String.class, 0).getClass() });
		String[] newArgs = Arrays.asList(args).subList(firstArg, args.length)
				.toArray(new String[0]);
		try {
			main.invoke(null, new Object[] { newArgs });
		} catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}

	/**
	 * ${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
	 *
	 * @param jarName
	 * @param runClass
	 * @param args
	 * @throws Throwable
	 */
	public static Job runJar(Configuration conf, String jarName,AbstractTool toolClass, String[] args) throws Throwable {
		File file = new File(jarName);
		File tmpDir = new File(conf.get("hadoop.tmp.dir"));
		tmpDir.mkdirs();
		if (!tmpDir.isDirectory()) {
			System.err.println("Mkdirs failed to create " + tmpDir);
			System.exit(-1);
		}
		final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
		workDir.delete();
		workDir.mkdirs();
		if (!workDir.isDirectory()) {
			System.err.println("Mkdirs failed to create " + workDir);
			System.exit(-1);
		}
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					FileUtil.fullyDelete(workDir);
				} catch (Exception e) {
				}
			}
		});
		unJar(file, workDir);
		ArrayList<URL> classPath = new ArrayList<URL>();
		classPath.add(new File(workDir + "/").toURI().toURL());
		classPath.add(file.toURI().toURL());
		classPath.add(new File(workDir, "classes/").toURI().toURL());
		File[] libs = new File(workDir, "lib").listFiles();
		if (libs != null) {
			for (int i = 0; i < libs.length; i++) {
				classPath.add(libs[i].toURI().toURL());
			}
		}
		ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
		initJavaLibrayPath(conf); // 初始化java libray path
		Thread.currentThread().setContextClassLoader(loader);
		run(conf, toolClass, args);
		return toolClass.getJob();
	}

	/**
	 *
	 * @param conf
	 * @param tool
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static int run(Configuration conf, Tool tool, String[] args)
			throws Exception {
		if (tool.getConf() == null) {
			tool.setConf(conf);
		}
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] toolArgs = parser.getRemainingArgs();
		return tool.run(toolArgs);
	}

	/**
	 * 初始化java libray path
	 *
	 * @param conf
	 * @throws IOException
	 */
	public static void initJavaLibrayPath(Configuration conf)
			throws IOException {
		String hadoopHome = conf.get("com.sky.hadoop.home", "/opt/hadoop/");
		String javaLibPath = hadoopHome + "/lib/native/" + PlatformName.PLATFORM_NAME;
		addDir(javaLibPath);
	}
}
