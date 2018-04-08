//package com.wplay.hadoop.manager;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.Manager;
//
///**
// *
// * @author James
// *
// */
//public class HadoopManager extends Manager {
//
//	/**
//	 *
//	 * @param conf
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	public HadoopManager(Configuration conf) throws IOException, InterruptedException {
//		super(conf);
//	}
//
//	public static void main(String args[]){
//		String usage = "HadoopManager Usage: -stop <JobID>";
//		if(args.length < 2){
//			System.err.println(usage);
//			return;
//		}
//	}
//}