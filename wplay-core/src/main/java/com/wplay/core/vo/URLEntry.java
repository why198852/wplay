package com.wplay.core.vo;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author James
 *
 */
public class URLEntry {

	private Map<String,String> params = new HashMap<String,String>();
	private String host;
	private String action;
	private int port;
	
	public URLEntry(String host,int port,String action){
		this.host = host;
		this.port = port;
		this.action = action;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public URLEntry addParams(String key,Object value){
		if(value != null){
			params.put(key, value.toString());
		}
		return this;
	}
	
	public void remove(String key){
		params.remove(key);
	}
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public String toStr() throws IOException{
		StringBuffer sb = new StringBuffer();
		sb.append("http://").append(host).append(":").append(port);
		if(action.startsWith("/")){
			sb.append(action).append("?");
		}else{
			sb.append("/").append(action).append("?");
		}
		for(Map.Entry<String,String> entry : params.entrySet()){
			sb.append(entry.getKey()).append("=").append(java.net.URLEncoder.encode(entry.getValue(), "UTF-8")).append("&");
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public URL toURL()  throws IOException{
		return new URL(toStr());
	}
	
	public static void main(String args[]) throws IOException{
		URLEntry uu = new URLEntry("localhost",8080,"get");
		uu.addParams("start", "333").addParams("end", "88");
		System.out.println(uu.toURL());
	}
}
