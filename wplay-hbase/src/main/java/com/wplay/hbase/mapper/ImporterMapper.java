package com.wplay.hbase.mapper;

import com.wplay.hbase.solr.DataDesc;
import com.wplay.hbase.solr.FieldDesc;
import com.wplay.hbase.solr.RowKeyDesc;
import com.wplay.core.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	public static final char DATA_OUT_SPLIT_DEF = '\001';
	public static final String DATA_OUT_SPLIT_STR = new String(new char[]{DATA_OUT_SPLIT_DEF});
    protected Pattern pattern = null;
    protected MultipleOutputs<NullWritable, Text> mos = null;
    private DataDesc data;
    protected String tableName;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration config = context.getConfiguration();
	this.data = DataDesc.readFromBase64(config.get(Constants.MR_DATA_CONF));
	String dataSplit = this.data.getDatasplit();
	if("\\001".equalsIgnoreCase(dataSplit)){
		dataSplit = DATA_OUT_SPLIT_STR;
	}
	pattern = Pattern.compile(dataSplit);
	mos = new MultipleOutputs(context);
	this.tableName = config.get(Constants.COMMAND_MR_TABLE);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	String[] valueSplit = pattern.split(value.toString(), -1);
	try {
	    byte[] rowkey = buildRowKey(valueSplit, this.data);
	    ImmutableBytesWritable itbw = new ImmutableBytesWritable(rowkey);
	    List<KeyValue> putter = this.buildKeyValue(valueSplit, data, rowkey);
	    for (int i = 0; i < putter.size(); i++) {
		context.write(itbw, putter.get(i));
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    context.getCounter("ERROR", "error").increment(1);
	    mos.write(Constants.ERROR_FILE_OUTPUT, NullWritable.get(), value, "_error");
	}
    }

    @Override
    public void cleanup(Context context) throws IOException,
	    InterruptedException {
	try {
	    if (mos != null) {
		mos.close();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    protected byte[] buildRowKey(String[] vs, DataDesc data) {
	RowKeyDesc row = data.getRowKey();
	String rowKeySplit = row.getRowsplit();
	List<String> fields = row.getFields();
	StringBuilder sbRowKey = new StringBuilder();
	for (String field : fields) {
	    for (FieldDesc dataField : data.getFields().values()) {
		if(vs.length > dataField.getOrder()){
		    String value = vs[dataField.getOrder()];
		    if (dataField.hasChild()) {
			String cVS[] = value.split(dataField.getChildSplit(),
				-1);
			for (FieldDesc child : dataField.getChilds().values()) {
			    this.buildChildRowKey(sbRowKey, cVS, child, field,
				    rowKeySplit);
			}
		    } else if (field.equals(dataField.getName())) {
			value = this.filterRowKey(field, dataField, value,rowKeySplit);
			sbRowKey.append(value).append(rowKeySplit);
		    }
		}
	    }
	}
	sbRowKey.setLength(sbRowKey.length() - rowKeySplit.length());
	return Bytes.toBytes(sbRowKey.toString());
    }
    
    /**
     * 
     * @param sbRowKey
     * @param vs
     * @param dataField
     * @param rowKeyField
	 * @param split
     */
    private void buildChildRowKey(StringBuilder sbRowKey, String vs[], FieldDesc dataField,String rowKeyField,String split) {
	if(vs.length > dataField.getOrder()){
	    String cValue = vs[dataField.getOrder()];
	    if (dataField.hasChild()) {
		String cVS[] = cValue.split(dataField.getChildSplit(), -1);
		for (FieldDesc cChild : dataField.getChilds().values()) {
		    this.buildChildRowKey(sbRowKey, cVS, cChild, rowKeyField,
			    split);
		}
	    } else {
		if (rowKeyField.equals(dataField.getName())) {
		    cValue = this.filterRowKey(rowKeyField, dataField, cValue,split);
		    sbRowKey.append(cValue).append(split);
		}
	    }
	}

    }

    protected List<KeyValue> buildKeyValue(String[] vs, DataDesc data,
	    byte[] rowkey) {
	List<KeyValue> kvs = new ArrayList<KeyValue>();
	for (FieldDesc field : data.getFields().values()) {
	    if(vs.length > field.getOrder()){
		String value = vs[field.getOrder()];
		if (field.hasChild()) {
		    String cVS[] = value.split(field.getChildSplit(), -1);
		    if(cVS.length == 1){//是默认数据
			 KeyValue kv = new KeyValue(rowkey, Bytes.toBytes(field.getFamily()), Bytes.toBytes(field.getDef()), Bytes.toBytes(value));
			 kv = this.filterColumn(field, kv, value);
			 kvs.add(kv);
		    }else{
			for (FieldDesc child : field.getChilds().values()) {
			    this.buildChild(kvs, cVS, child, rowkey);
			}
		    }
		} else {
		    KeyValue kv = new KeyValue(rowkey, Bytes.toBytes(field
			    .getFamily()), Bytes.toBytes(field.getName()),
			    Bytes.toBytes(value));
		    kv = this.filterColumn(field, kv, value);
		    kvs.add(kv);
		}
	    }
	   
	}
	return kvs;
    }

    /**
     * 
     * @param kvs
     * @param vs
     * @param child
     * @param rowkey
     */
    private void buildChild(List<KeyValue> kvs, String vs[], FieldDesc child, byte[] rowkey) {
	if(vs.length > child.getOrder()){
	    String cValue = vs[child.getOrder()];
	    if (child.hasChild()) {
		String cVS[] = cValue.split(child.getChildSplit(), -1);
		for (FieldDesc cChild : child.getChilds().values()) {
		    this.buildChild(kvs, cVS, cChild, rowkey);
		}
	    } else {
		KeyValue kv = new KeyValue(rowkey, Bytes.toBytes(child
			.getFamily()), Bytes.toBytes(child.getName()),
			Bytes.toBytes(cValue));
		kv = this.filterColumn(child, kv, cValue);
		kvs.add(kv);
	    } 
	}
    }

    /**
     * 
     * @param rowkeyField
     * @param dataField
     * @param value
     * @return
     */
    protected String filterRowKey(String rowkeyField,FieldDesc dataField,String value,String rowKeySplit){
	return value;
    }
    
    /**
     * 
     * @param field
     * @param kv
     * @param value
     * @return
     */
    protected KeyValue filterColumn(FieldDesc field, KeyValue kv, String value) {
	return kv;
    }
}
