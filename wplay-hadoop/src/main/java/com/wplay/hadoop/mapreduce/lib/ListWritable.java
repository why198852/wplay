package com.wplay.hadoop.mapreduce.lib;

import com.wplay.hadoop.util.WriteableUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListWritable implements Writable {

    private List<Writable> list = new ArrayList<Writable>();

    /**
     * 
     */
    public ListWritable() {
    }

    /**
     * 
     * @param list
     */
    public ListWritable(List<Writable> list) {
	this.list = list;
    }

    /**
     * @return the list
     */
    public List<? extends Writable> getList() {
	return list;
    }

    /**
     * 
     * @param writables
     */
    public void addAll(List<? extends Writable> writables) {
	this.list.addAll(writables);
    }
    
    public void add(Writable wriable){
	this.list.add(wriable);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	int size = in.readInt();
	for (int i = 0; i < size; i++) {
	    try {
		list.add(WriteableUtil.readObject(in));
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(list.size());
	for (Writable writable : list) {
	    WriteableUtil.writeObject(writable, out);
	}
    }

}
