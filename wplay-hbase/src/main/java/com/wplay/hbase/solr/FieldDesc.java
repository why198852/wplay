package com.wplay.hbase.solr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author James
 * 
 */
public class FieldDesc implements WritableComparable<FieldDesc> {

    private int order;
    private String family;
    private String name;
    private Map<String, FieldDesc> childs;
    private String childSplit;
    private String def; // 如果数据中没有子字段，则把这个值设置为默认字段
    private String description;

    /**
     * @return the order
     */
    public int getOrder() {
	return order;
    }

    /**
     * @param order
     *            the order to set
     */
    public void setOrder(int order) {
	this.order = order;
    }

    /**
     * @return the family
     */
    public String getFamily() {
	return family;
    }

    /**
     * @param family
     *            the family to set
     */
    public void setFamily(String family) {
	this.family = family;
    }

    /**
     * @return the name
     */
    public String getName() {
	return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
	this.name = name;
    }

    /**
     * @return the childs
     */
    public Map<String, FieldDesc> getChilds() {
	return childs;
    }

    /**
     * @param childs
     *            the childs to set
     */
    public void setChilds(Map<String, FieldDesc> childs) {
	this.childs = childs;
    }

    /**
     * @return the childSplit
     */
    public String getChildSplit() {
	return childSplit;
    }

    /**
     * @param childSplit
     *            the childSplit to set
     */
    public void setChildSplit(String childSplit) {
	this.childSplit = childSplit;
    }

    /**
     * 
     * @return
     */
    public boolean hasChild() {
	return this.childs != null && !this.childs.isEmpty();
    }

    /**
     * @return the def
     */
    public String getDef() {
	return def;
    }

    /**
     * @param def
     *            the def to set
     */
    public void setDef(String def) {
	this.def = def;
    }

    /**
     * @return the description
     */
    public String getDescription() {
	return description;
    }

    /**
     * @param description
     *            the description to set
     */
    public void setDescription(String description) {
	this.description = description;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	StringBuilder builder = new StringBuilder();
	builder.append("FieldDesc [order=");
	builder.append(order).append("\n");
	builder.append(", family=");
	builder.append(family).append("\n");
	builder.append(", name=");
	builder.append(name).append("\n");
	builder.append(", childs=");
	builder.append(childs).append("\n");
	builder.append(", childSplit=");
	builder.append(childSplit).append("\n");
	builder.append("]");
	return builder.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	this.order = WritableUtils.readVInt(in);
	this.family = WritableUtils.readString(in);
	this.name = WritableUtils.readString(in);
	int size = WritableUtils.readVInt(in);
	this.childs = new HashMap<String, FieldDesc>();
	for (int i = 0; i < size; i++) {
	    FieldDesc child = FieldDesc.read(in);
	    this.childs.put(child.getName(), child);
	}
	this.childSplit = WritableUtils.readString(in);
	this.def = WritableUtils.readString(in);
	this.description = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	WritableUtils.writeVInt(out, order);
	WritableUtils.writeString(out, family);
	WritableUtils.writeString(out, name);
	if (this.hasChild()) {
	    WritableUtils.writeVInt(out, this.childs.size());
	    for (FieldDesc field : this.childs.values()) {
		field.write(out);
	    }
	} else {
	    WritableUtils.writeVInt(out, 0);
	}
	WritableUtils.writeString(out, childSplit);
	WritableUtils.writeString(out, def);
	WritableUtils.writeString(out, description);
    }

    public static final FieldDesc read(DataInput in) throws IOException {
	FieldDesc field = new FieldDesc();
	field.readFields(in);
	return field;
    }

    @Override
    public int compareTo(FieldDesc o) {
	if (o != null) {
	    return this.order - o.order;
	} else {
	    return 1;
	}
    }

    /**
     * <field order="1" hasChild="true" def="C" family="I" desc="C">
     * <datasplit>,</datasplit> <fields> <field order="0" family="I"
     * desc="B">B</field> <field order="1" family="I" desc="C">C</field>
     * </fields> </field>
     * 
     * @param doc
     * @return
     */
    public Element buildNode(Document doc) {
	Element fieldNode = doc.createElement(DataUtil.CONF_FIELD);
	fieldNode.setAttribute(DataUtil.CONF_ORDER, Integer.toString(getOrder()));
	fieldNode.setAttribute(DataUtil.CONF_FAMILY, getFamily());
	fieldNode.setAttribute(DataUtil.CONF_DESC, getDescription());
	if (hasChild()) {
	    fieldNode.setAttribute(DataUtil.CONF_HAS_CHILD, "true");
	    fieldNode.setAttribute(DataUtil.CONF_DEF, getDef());
	    Element datasplitNode = doc.createElement(DataUtil.CONF_DATA_SPLIT);
	    datasplitNode.appendChild(doc.createTextNode(getChildSplit()));
	    fieldNode.appendChild(datasplitNode);

	    Element fieldsNode = doc.createElement(DataUtil.CONF_FIELDS);
	    for (FieldDesc child : getChilds().values()) {
		fieldsNode.appendChild(child.buildNode(doc));
	    }
	    fieldNode.appendChild(fieldsNode);
	} else {
	    fieldNode.appendChild(doc.createTextNode(getName()));
	}
	return fieldNode;
    }
}
