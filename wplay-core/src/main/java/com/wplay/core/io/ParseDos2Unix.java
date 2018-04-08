package com.wplay.core.io;

import com.wplay.core.util.XmlConfigUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * 处理GZ文件回车问题
 * @author user
 *
 */
public class ParseDos2Unix extends Configured implements Configurable{
	private static final Logger LOG = Logger.getLogger(ParseDos2Unix.class);
	public static final String PARSED = ".parsed"; // 正在转换中。。。
	private static final int ENTER = '\r';
	public static final int BUFFER_SIZE = 8096;
	private CompressionCodecFactory codecFactory;

	public ParseDos2Unix(Configuration conf){
		super(conf);
		this.codecFactory = new CompressionCodecFactory(conf);
		Decompressor decompressor = Algorithm.GZ.getDecompressor();
		LOG.info("Init native GZ decompressor " + decompressor);
	}
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String usage = "Usage : ParseDos2Unix <src> <dest>";
		if(args.length < 2){
			System.out.println(usage);
			System.exit(-1);
		}
		Path src = new Path(args[0]);
		Path dest = new Path(args[1]);

		FileSystem fs = FileSystem.get(XmlConfigUtil.create());
		Configuration conf = XmlConfigUtil.create();
		ParseDos2Unix dos2Unix = new ParseDos2Unix(conf);
		dos2Unix.parseEnter(fs,src,dest);
	}

	/**
	 *
	 * @param fs
	 * @param input
	 * @param output
	 * @throws IOException
	 */
	public void parseEnter(FileSystem fs,Path input,Path output) throws IOException{
		FileStatus fileStatus = fs.getFileStatus(input);
		Path unixPath = output;
		if (fs.exists(output)) {
		    unixPath = new Path(output, input.getName());
		}
		if (fileStatus.isDirectory()) {
		    LOG.info("Input " + input + " is dir will mkdir " + unixPath);
		    fs.mkdirs(unixPath);
		    FileStatus[] files = fs.listStatus(input);
		    for (FileStatus file : files) {
		    	parseEnter(fs,file.getPath(), unixPath);
		    }
		} else {
			InputStream in = fs.open(input);
			CompressionCodec codec = codecFactory.getCodec(input);
			InputStream codecIn = in;
			if(codec != null){
				Decompressor decompressor = CodecPool.getDecompressor(codec);
				codecIn = codec.createInputStream(in, decompressor);
				if(unixPath.getName().endsWith(codec.getDefaultExtension())){
					unixPath = new Path(unixPath.getParent(),unixPath.getName().substring(0, unixPath.getName().length() - codec.getDefaultExtension().length()));
				}
			}
		    fs.mkdirs(unixPath.getParent());
		    Path parsed = new Path(unixPath.getParent(),unixPath.getName() + PARSED);
		    LOG.info("Will parse " + input + " to " + parsed);
		    OutputStream out = fs.create(parsed);
		    output(codecIn, out); // 输出
		    LOG.info("Dos2unix success");
		    LOG.info("Will rename " + parsed + " to " + unixPath);
		    fs.rename(parsed, unixPath);
		}
	}

	/**
	 * dos2unix
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	private static final void output(InputStream in ,OutputStream out) throws IOException{
		byte[] b = new byte[BUFFER_SIZE];int n;
		while((n = in.read(b)) != -1){
			for(int i = 0;i < n;i ++){
				if(ENTER != b[i]){
					out.write(b[i]);
				}
			}
		}
		in.close();
		out.close();
	}
}
