package com.wplay.core.basic;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * lib.dir = ./lib
   jetty.lib.dir = ./jetty
   hadoop.lib.dir = ./hadoop
   solr.lib.dir = ./solr
   slf.lib.dir=./slf
   
 * @author James
 *
 */
public class GenLib {

    private static final String BASE_PATH = "/Users/James/workspace/bigdata-ZC3-cdh4";
   
    private static final Map<String,String> libs = new HashMap<String,String>();
    static{
	libs.put("lib.dir","/lib");
	libs.put("jetty.lib.dir","/jetty");
	libs.put("hadoop.lib.dir","/hadoop");
	libs.put("solr.lib.dir","/solr");
	libs.put("slf.lib.dir","/slf");
    }
    
    
    
    /**
     * @param args
     */
    public static void main(String[] args) {
	StringBuilder sb = new StringBuilder();
	for(Entry<String,String> entry : libs.entrySet()){
	    String key = "${" + entry.getKey() + "}";
	    File file = new File(BASE_PATH,entry.getValue());
	    File[] files = file.listFiles(
		    new FilenameFilter() {
		        @Override
		        public boolean accept(File dir, String name) {
		    	return name.endsWith(".jar");
		        }
		    }
		    );
	    
	    for(File f : files){
		sb.append(key).append("/").append(f.getName()).append(":");
	    }
	}
	System.out.println(sb.toString());
    }

}
