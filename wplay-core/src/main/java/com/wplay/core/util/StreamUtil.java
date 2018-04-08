package com.wplay.core.util;

import org.xml.sax.InputSource;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Pattern;

public class StreamUtil {
	
	private static final String USER_HOME = "user.home";
	private static final String USER_NAME = "user.name";
	public static final Pattern pattern =  Pattern.compile("attachment;.*filename=(.+)",Pattern.CASE_INSENSITIVE);

	public static final int BUFFER_SIZE = 2048; 
	
	/**
	 * 
	 * @return
	 */
	public static File getHome(){
		return new File(System.getProperty(USER_HOME));
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getUser(){
		return System.getProperty(USER_NAME);
	}
	
	public static byte[] getByte(InputStream in) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		output(in,out);
		return out.toByteArray();
	}
	
	public static byte[] getByte(URL url) throws IOException{
		URLConnection urlc = url.openConnection();
		byte[] content = getByte(urlc.getInputStream());
		return parseContent(content, urlc.getContentEncoding());
	}
	
	public static InputSource getInputSource(URL url) throws IOException{
		byte[] content = getByte(url);
		if(content != null){
			return new InputSource(new ByteArrayInputStream(content));
		}else{
			return null;
		}
	}
	
	public static byte[] getByte(String url) throws IOException{
		return getByte(new URL(url));
	}
	
	public static void output(byte[] content,OutputStream out) throws IOException{
		output(new ByteArrayInputStream(content),out);
	}
	
	public static void out(URL url,OutputStream...out) throws IOException{
		InputStream in = url.openStream();
		byte[] b = new byte[2048];int n;
		while((n = in.read(b)) != -1){
			for(OutputStream o : out){
				o.write(b,0,n);
			}
		}
		in.close();
		for(OutputStream o : out){
			o.flush();
			o.close();
		}
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	public static void output(InputStream in,OutputStream out) throws IOException{
		output(in,out,true);
	}
	
	public static void output(InputStream in,OutputStream out,int bufferSize) throws IOException{
	    output(in,out,true,bufferSize);
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param closeOut
	 * @throws IOException
	 */
	public static void output(InputStream in,OutputStream out,boolean closeOut) throws IOException{
		output(in,out,closeOut,true);
	}
	
	public static void output(InputStream in,OutputStream out,boolean closeOut,int bufferSize) throws IOException{
		output(in,out,closeOut,true,bufferSize);
	}
	
	public static void output(InputStream in,OutputStream out,boolean closeOut,boolean closeIn) throws IOException{
	    output(in,out,closeOut,closeIn,BUFFER_SIZE);
	}
	
	public static void output(InputStream in,OutputStream out,boolean closeOut,boolean closeIn,int bufferSize) throws IOException{
		byte[] b = new byte[bufferSize];int n;
		while((n = in.read(b)) != -1){
			out.write(b,0,n);
		}
		out.flush();
		if(closeIn){
			in.close();
		}
		if(closeOut){
			out.close();
		}
	}
	
	/**
	 * 
	 * @param start
	 * @param in
	 * @param out
	 * @param limit
	 * @throws IOException
	 */
	public static void output(long start,InputStream in,OutputStream out,long limit) throws IOException{
		in.skip(start);
		byte[] b = new byte[2048];int n;
		long l = 0;
		while((n = in.read(b)) != -1){
			l += n;
			if(l < limit){
				out.write(b,0,n);
			}else{
				out.write(b,0,(int) (limit - (l - n)));
			}
		}
		out.flush();
		in.close();out.close();
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param limit
	 * @throws IOException
	 */
	public static void output(InputStream in,OutputStream out,long limit) throws IOException{
		byte[] b = new byte[2048];int n;
		long l = 0;
		while((n = in.read(b)) != -1){
			l += n;
			if(l < limit){
				out.write(b,0,n);
			}else{
				out.write(b,0,(int) (limit - (l - n)));
			}
		}
		out.flush();
		in.close();out.close();
	}
	
	/**
	 * 
	 * @param start
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	public static void output(long start,InputStream in,OutputStream out) throws IOException{
		in.skip(start);
		byte[] b = new byte[2048];int n;
		while((n = in.read(b)) != -1){
			out.write(b,0,n);
		}
		out.flush();
		in.close();out.close();
	}
	
	public static void output(String content,OutputStream out) throws IOException{
		output(new ByteArrayInputStream(content.getBytes()),out);
	}
	
	public static void output(String content,File out) throws IOException{
		output(new ByteArrayInputStream(content.getBytes()),out);
	}
	
	public static void output(InputStream in,File out) throws IOException{
		if(!out.getParentFile().exists()){
			out.getParentFile().mkdirs();
		}
		output(in,new FileOutputStream(out));
	}
	
	public static void output(URL url,OutputStream out) throws IOException{
		InputStream in = url.openStream();
		output(in,out);
	}
	
	/**
	 * 
	 * @param url
	 * @param out
	 * @return 下载文件名
	 * @throws IOException
	 */
	public static String output(URL url,File out) throws IOException{
		URLConnection uconn = url.openConnection();
		InputStream in = uconn.getInputStream();
		String file = uconn.getURL().getPath();
		if(!out.getParentFile().exists()){
			out.getParentFile().mkdirs();
		}
		output(in,new FileOutputStream(out));
		if(file != null){
			int index = file.lastIndexOf('.');
			return index > 0 ? file.substring(index):file;
		}else{
			return null;
		}
	}
	
	public static void output(Object inputSource,OutputStream out) throws IOException {
		if(inputSource instanceof InputStream){
			output((InputStream)inputSource,out);
		}else if(inputSource instanceof byte[]){
			output((byte[])inputSource,out);
		}else if(inputSource instanceof URL){
			output((URL)inputSource,out);
		}else if(inputSource instanceof String){
			output((String)inputSource,out);
		}else{
			throw new IOException("Don't support this source");
		}
	}
	
	public static byte[] parseContent(byte[] content,String contentEncoding) throws IOException {
		if (content != null) {
			if ("gzip".equals(contentEncoding)
					|| "x-gzip".equals(contentEncoding)) {
				return  processGzipEncoded(content);
			} else if ("deflate".equals(contentEncoding)) {
				return  processDeflateEncoded(content);
			}else{
				return content;
			}
		}
		return null;
	}

	public static byte[] processGzipEncoded(byte[] compressed)
			throws IOException {
		byte[] content;
			content = GZIPUtils.unzipBestEffort(compressed);
		if (content == null)
			throw new IOException("unzipBestEffort returned null");
		return content;
	}
	
	public static byte[] processDeflateEncoded(byte[] compressed)
	throws IOException {
		byte[] content = DeflateUtils.inflateBestEffort(compressed);
		if (content == null)
			throw new IOException("inflateBestEffort returned null");
		return content;
	}
	
	public static boolean isImage(String url){
		try{
			URL u = new URL(url);
			URLConnection uc = u.openConnection();
			String contentType = uc.getContentType();
			return contentType.startsWith("image");
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static BufferedReader getBufferedReader(File file) throws IOException{
		return getBufferedReader(new FileInputStream(file));
	}

	/**
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static BufferedReader getBufferedReader(InputStream in) throws IOException{
		return getBufferedReader(in,"utf-8");
	}
	
	/**
	 * 
	 * @param file
	 * @param encode
	 * @return
	 * @throws IOException
	 */
	public static BufferedReader getBufferedReader(File file,String encode) throws IOException{
		return getBufferedReader(new FileInputStream(file),encode);
	}
	
	/**
	 * 
	 * @param in
	 * @param encode
	 * @return
	 * @throws IOException
	 */
	public static BufferedReader getBufferedReader(InputStream in,String encode) throws IOException{
		return new BufferedReader(new InputStreamReader(in,encode));
	}
	
	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static BufferedReader getBufferedReader(String file) throws IOException{
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getPriKeyPath(){
		File file = getPriKeyFile();
		if(file != null){
			return file.toString();
		}
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public static File getPriKeyFile(){
		String userHome = System.getProperty("user.home");
		File sshHome = new File(userHome,".ssh");
		if(sshHome.exists()){
			File rsaFile = new File(sshHome,"id_rsa");
			if(rsaFile.exists()){
				return rsaFile;
			}else{
				File priFiles[] = sshHome.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.startsWith("id_") && !name.endsWith(".pub");
					}
				});
				if(priFiles != null && priFiles.length > 0){
					return priFiles[0];
				}
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public static File getTmpDir(){
		return new File(System.getProperty("java.io.tmpdir"));
	}
}