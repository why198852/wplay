package com.wplay.hbase.solr;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Êý¾ÝÃèÊö
 * @author James
 *
 */
public class DataDesc implements Writable{

    private String name;
    private String datasplit;
    private Map<String,FieldDesc> fields;
    private RowKeyDesc rowKey;
    private String description;
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }
    /**
     * @return the datasplit
     */
    public String getDatasplit() {
        return datasplit;
    }
    /**
     * @param datasplit the datasplit to set
     */
    public void setDatasplit(String datasplit) {
        this.datasplit = datasplit;
    }
    /**
     * @return the fields
     */
    public Map<String, FieldDesc> getFields() {
        return fields;
    }
    /**
     * @param fields the fields to set
     */
    public void setFields(Map<String, FieldDesc> fields) {
        this.fields = fields;
    }
    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }
    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	StringBuilder builder = new StringBuilder();
	builder.append("DataDesc [name=");
	builder.append(name).append("\n");
	builder.append(", datasplit=");
	builder.append(datasplit).append("\n");
	builder.append(", fields=");
	builder.append(fields).append("\n");
	builder.append(", rowKey=");
	builder.append(rowKey).append("\n");
	builder.append("]");
	return builder.toString();
    }
    /**
     * @return the rowKey
     */
    public RowKeyDesc getRowKey() {
        return rowKey;
    }
    /**
     * @param rowKey the rowKey to set
     */
    public void setRowKey(RowKeyDesc rowKey) {
        this.rowKey = rowKey;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
	this.name = WritableUtils.readString(in);
	this.datasplit = WritableUtils.readString(in);
	this.fields = new HashMap<String, FieldDesc>();
	int size = WritableUtils.readVInt(in);
	for(int i = 0;i < size;i ++){
	    FieldDesc field = FieldDesc.read(in);
	    fields.put(field.getName(), field);
	}
	this.rowKey = RowKeyDesc.read(in);
	this.description = WritableUtils.readString(in);
    }
    @Override
    public void write(DataOutput out) throws IOException {
	WritableUtils.writeString(out, name);
	WritableUtils.writeString(out, datasplit);
	if(this.fields == null || this.fields.isEmpty()){
	    WritableUtils.writeVInt(out, 0);
	}else{
	    WritableUtils.writeVInt(out, fields.size());
	    for(FieldDesc field : fields.values()){
		field.write(out);
	    }
	}
	if(this.rowKey == null){
	    this.rowKey = new RowKeyDesc();
	}
	this.rowKey.write(out);
	WritableUtils.writeString(out, description);
    }
    
    /**
     * 
     * @return
     * @throws IOException
     */
    public String toBase64() throws IOException{
	 ByteArrayOutputStream out = new ByteArrayOutputStream();
	 DataOutputStream dos = new DataOutputStream(out);
	 this.write(dos);
	 dos.close();
	 return Base64.encodeBytes(out.toByteArray());
    }
    
    /**
     * 
     * @param base64
     * @return
     * @throws IOException
     */
    public static final DataDesc readFromBase64(String base64) throws IOException{
	byte[] b = Base64.decode(base64);
	ByteArrayInputStream in = new ByteArrayInputStream(b);
	DataInputStream dis = new DataInputStream(in);
	DataDesc data = new DataDesc();
	data.readFields(dis);
	dis.close();
	return data;
    }
    
    /**
     * 
     * 
     * 	<data name="T2" desc="T2">
	    <datasplit>\t</datasplit>
	    <fields>
	      <field order="0" family="I"  desc="IP">IP</field>
	      <field order="1" hasChild="true" def="C" family="I" desc="C">
	      	<datasplit>,</datasplit>
	      	<fields>
	      		<field order="0" family="I" desc="B">B</field>
	      		<field order="1" family="I" desc="C">C</field>
	      	</fields>
	      </field>
	    </fields>
	    <rowkey>
	      <field>IP</field>
	      <rowsplit>|</rowsplit>
	    </rowkey>
	</data>
     * @param doc
     * @return
     */
    public Element buildNode(Document doc){
	Element dataNode = doc.createElement(DataUtil.CONF_DATA);
	dataNode.setAttribute("name", getName());
	dataNode.setAttribute("desc",getDescription());
	
	Element dataSplitNode = doc.createElement("datasplit");
	dataSplitNode.appendChild(doc.createTextNode(getDatasplit()));
	dataNode.appendChild(dataSplitNode);
	
	Element fieldsNode = doc.createElement(DataUtil.CONF_FIELDS);
	for(FieldDesc field : getFields().values()){
	    fieldsNode.appendChild(field.buildNode(doc));
	}
	dataNode.appendChild(fieldsNode);
	
	dataNode.appendChild(getRowKey().buildNode(doc));
	return dataNode;
    }
}
