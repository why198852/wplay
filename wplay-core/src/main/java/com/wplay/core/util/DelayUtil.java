package com.wplay.core.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

/**
 * 
 * @author James
 *
 */
public class DelayUtil {
	
	public static final Logger LOG = Logger.getLogger(DelayUtil.class);
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private long start;
	private long end;
	
	public long getStart() {
		return start;
	}
	
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	
	public void printStart(String type){
		LOG.info(type + " : starting at " + sdf.format(Long.valueOf(start)));
	}
	
	public void printEnd(String type){
		LOG.info(type + " : finished at " + sdf.format(Long.valueOf(end)) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}
	
	/**
	 * 
	 * @param log
	 * @param type
	 */
	public void printStart(Logger log,String type){
		log.info(type + " : starting at " + sdf.format(Long.valueOf(start)));
	}
	
	/**
	 * 
	 * @param log
	 * @param type
	 */
	public void printEnd(Logger log,String type){
		log.info(type + " : finished at " + sdf.format(Long.valueOf(end)) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}
}