package com.wplay.hbase.util;

import org.apache.hadoop.io.Writable;

import java.io.IOException;

public abstract class HbaseWritable implements Writable {

    /**
     * 
     * @return
     * @throws IOException
     */
    public byte[] toByte() throws IOException {
	return WriteableUtil.toBytes(this);
    }

    public abstract byte[] getRowKey();

    /**
     * 
     * @param value
     * @throws IOException
     */
    public void readByte(byte[] value) throws IOException {
	WriteableUtil.read(this, value);
    }
}
