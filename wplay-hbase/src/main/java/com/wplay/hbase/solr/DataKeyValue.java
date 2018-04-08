package com.wplay.hbase.solr;

import org.apache.hadoop.hbase.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author James
 *
 */
public class DataKeyValue {

    private Map<String, String> maps = new HashMap<String, String>();
    private List<KeyValue> kvs = new ArrayList<KeyValue>();

    /**
     * 
     * @param name
     * @param value
     */
    public void put(String name,String value){
	this.maps.put(name, value);
    }
    
    public String getValue(String name){
	return maps.get(name);
    }
    
    /**
     * 
     * @param kv
     */
    public void add(KeyValue kv){
	this.kvs.add(kv);
    }
    /**
     * @return the maps
     */
    public Map<String, String> getMaps() {
	return maps;
    }

    /**
     * @return the kvs
     */
    public List<KeyValue> getKvs() {
	return kvs;
    }

}
