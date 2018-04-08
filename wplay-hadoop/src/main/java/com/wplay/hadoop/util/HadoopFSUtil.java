/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplay.hadoop.util;

import com.wplay.core.util.StreamUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.MapFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class HadoopFSUtil {

    public static final String HIDDEN_FILE = "_";
    public static final String MD5_SUFFIX = ".md5";

    public static final PathFilter hiddenFileFilter = new PathFilter() {
	public boolean accept(Path p) {
	    String name = p.getName();
	    return (!name.startsWith(HIDDEN_FILE)) && (!name.startsWith("."));
	}
    };

    /**
     * Returns PathFilter that passes all paths through.
     */
    public static PathFilter getPassAllFilter() {
	return new PathFilter() {
	    public boolean accept(Path arg0) {
		return true;
	    }
	};
    }

    /**
     * Returns PathFilter that passes directories through.
     */
    public static PathFilter getPassDirectoriesFilter(final FileSystem fs) {
	return new PathFilter() {
	    public boolean accept(final Path path) {
		try {
		    return fs.getFileStatus(path).isDirectory();
		} catch (IOException ioe) {
		    return false;
		}
	    }
	};
    }

    /**
     * Turns an array of FileStatus into an array of Paths.
     */
    public static Path[] getPaths(FileStatus[] stats) {
	if (stats == null) {
	    return null;
	}
	if (stats.length == 0) {
	    return new Path[0];
	}
	Path[] res = new Path[stats.length];
	for (int i = 0; i < stats.length; i++) {
	    res[i] = stats[i].getPath();
	}
	return res;
    }

    /**
     * 
     * @param fs
     * @param dir
     * @return
     * @throws IOException
     */
    public static FileStatus[] listPath(FileSystem fs, Path dir)
	    throws IOException {
	return fs.listStatus(dir, hiddenFileFilter);
    }

    /**
     * 
     * @param src
     * @param dest
     * @param fs
     * @throws IOException
     */
    public static void copy(Path src, Path dest, FileSystem fs)
	    throws IOException {
	if (fs.exists(dest)) {
	    if (fs.isDirectory(src)) {
		Path copyTo = new Path(dest, src.getName());
		fs.mkdirs(copyTo);
		FileStatus[] files = fs.listStatus(src);
		for (FileStatus file : files) {
		    copy(file.getPath(), copyTo, fs);
		}
	    } else {
		InputStream in = fs.open(src);
		OutputStream out = fs.create(new Path(dest, src.getName()),true);//覆盖原来已经有的
		StreamUtil.output(in, out);
	    }
	} else {
	    if (fs.isDirectory(src)) {
		fs.mkdirs(dest);
		FileStatus[] files = fs.listStatus(src);
		for (FileStatus file : files) {
		    copy(file.getPath(), dest, fs);
		}
	    } else {
		InputStream in = fs.open(src);
		OutputStream out = fs.create(dest,true);//覆盖原来已经有的
		StreamUtil.output(in, out);
	    }
	}
    }

    public static class FileName {
	private String name;
	private String suffix;

	public FileName(){}
	
	/**
	 * @param name
	 * @param suffix
	 */
	public FileName(String name, String suffix) {
	    super();
	    this.name = name;
	    this.suffix = suffix;
	}

	/**
	 * @return the name
	 */
	public String getName() {
	    return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
	    this.name = name;
	}

	/**
	 * @return the suffix
	 */
	public String getSuffix() {
	    return suffix;
	}

	/**
	 * @param suffix
	 *            the suffix to set
	 */
	public void setSuffix(String suffix) {
	    this.suffix = suffix;
	}
    }

    /**
     * 
     * @param src
     * @param fs
     * @throws IOException
     */
    public static void genMd5(Path src, FileSystem fs) throws IOException {
	if (fs.isDirectory(src)) {
	    FileStatus fileStatuses[] = listPath(fs,src);
	    for(FileStatus fileStatus : fileStatuses){
		genMd5(fileStatus.getPath(),fs);
	    }
	} else {
	    InputStream in = fs.open(src);
	    MD5Hash md5 = MD5Hash.digest(in);
	    FileName name = getFileName(src.getName());
	    Path md5path = new Path(src.getParent(), HIDDEN_FILE + name.getName() + MD5_SUFFIX);
	    OutputStream out = fs.create(md5path);
	    out.write(md5.toString().getBytes());
	    out.close();
	}
    }

    public static FileName getFileName(String name) {
	String fileName = name;
	String suffix = "";
	int index = name.indexOf(".");
	if (index != -1) {
	    fileName = name.substring(0, index);
	    suffix = name.substring(index);
	}
	return new FileName(fileName,suffix);
    }

    /**
     * 
     * @param fs
     * @param dir
     * @param conf
     * @return
     * @throws IOException
     */
    public static MapFile.Reader[] getReaders(FileSystem fs, Path dir,
	    Configuration conf) throws IOException {
	Path[] names = FileUtil
		.stat2Paths(fs.listStatus(dir, hiddenFileFilter));
	Arrays.sort(names);
	MapFile.Reader[] parts = new MapFile.Reader[names.length];
	for (int i = 0; i < names.length; i++) {
	    parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
	}
	return parts;
    }


}
