package com.wplay.hbase.solr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author James
 *
 */
public class RowKeyDesc implements Writable{

    private String rowsplit;
    private List<String> fields;
    
    /**
     * @return the rowsplit
     */
    public String getRowsplit() {
        return rowsplit;
    }
    /**
     * @param rowsplit the rowsplit to set
     */
    public void setRowsplit(String rowsplit) {
        this.rowsplit = rowsplit;
    }
    /**
     * @return the fields
     */
    public List<String> getFields() {
        return fields;
    }
    /**
     * @param fields the fields to set
     */
    public void setFields(List<String> fields) {
        this.fields = fields;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	StringBuilder builder = new StringBuilder();
	builder.append("RowKeyDesc [rowsplit=");
	builder.append(rowsplit).append("\n");
	builder.append(", fields=");
	builder.append(fields).append("\n");
	builder.append("]");
	return builder.toString();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
	this.rowsplit = WritableUtils.readString(in);
	int size = WritableUtils.readVInt(in);
	this.fields = new ArrayList<String>();
	for(int i = 0;i < size;i ++){
	    this.fields.add(WritableUtils.readString(in));
	}
    }
    @Override
    public void write(DataOutput out) throws IOException {
	WritableUtils.writeString(out, rowsplit);
	if(this.fields == null || this.fields.isEmpty()){
	    WritableUtils.writeVInt(out, 0);
	}else{
	    WritableUtils.writeVInt(out, this.fields.size());
	    for(String field : fields){
		WritableUtils.writeString(out, field);
	    }
	}
    }
    
    public static final RowKeyDesc read(DataInput in) throws IOException {
	RowKeyDesc row = new RowKeyDesc();
	row.readFields(in);
	return row;
    }
    
    public String toRowkey(){
	if(fields != null){
	    StringBuilder sb = new StringBuilder();
	    for(String field : fields){
		sb.append(field).append(rowsplit);
	    }
	    sb.setLength(sb.length() - rowsplit.length());
	    return sb.toString();
	}
	return "";
    }
    /**
     * 	    <rowkey>
	      <field>IP</field>
	      <rowsplit>|</rowsplit>
	    </rowkey>
     * @param doc
     * @return
     */
    public Node buildNode(Document doc) {
	Element rowkeyNode = doc.createElement(DataUtil.CONF_ROWKEY);
	for(String field : getFields()){
	    Element fieldNode = doc.createElement(DataUtil.CONF_FIELD);
	    fieldNode.appendChild(doc.createTextNode(field));
	    rowkeyNode.appendChild(fieldNode);
	}
	Element rowkeySplitNode = doc.createElement(DataUtil.CONF_ROWSPLIT);
	rowkeySplitNode.appendChild(doc.createTextNode(getRowsplit()));
	rowkeyNode.appendChild(rowkeySplitNode);
	return rowkeyNode;
    }
}
