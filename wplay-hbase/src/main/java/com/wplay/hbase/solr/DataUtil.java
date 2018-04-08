package com.wplay.hbase.solr;

import com.wplay.core.util.StringUtil;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

/**
 * 
 * @author James
 * 
 */
public class DataUtil {
    public static final String CONF_DATAS = "datas";
    public static final String CONF_DATA = "data";
    
    
    public static final String CONF_DATA_SPLIT = "datasplit";
    public static final String CONF_FIELDS = "fields";
    public static final String CONF_FIELD = "field";
    public static final String CONF_HAS_CHILD = "hasChild";
    
    
    public static final String CONF_ROWKEY = "rowkey";
    public static final String CONF_ROWSPLIT = "rowsplit";
    
    public static final String CONF_ORDER = "order";
    public static final String CONF_FAMILY = "family";
    public static final String CONF_DEF = "def";
    public static final String CONF_DESC = "desc";
    
    public static final String CONF_RESOURCE = "/data-desc.xml";
    private static final Logger LOG = Logger.getLogger(DataUtil.class);
    private static final Map<String, DataDesc> datas = new HashMap<String, DataDesc>();

    static{
	try {
	    init();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
    
    public static final void init() throws Exception {
	datas.clear();
	init(CONF_RESOURCE);
    }

    public static final void init(String resource) throws SAXException,
	    IOException, ParserConfigurationException {
	InputStream in = DataUtil.class.getResourceAsStream(resource);
	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	factory.setIgnoringComments(true);
	factory.setNamespaceAware(true);
	try {
	    factory.setXIncludeAware(true);
	} catch (UnsupportedOperationException e) {
	    e.printStackTrace();
	}
	DocumentBuilder builder = factory.newDocumentBuilder();
	Document doc = builder.parse(in);
	Element root = doc.getDocumentElement();
	if (!CONF_DATAS.equals(root.getTagName())) {
	    LOG.error("bad conf file : top-level element not <datas>");
	}
	NodeList nodes = root.getChildNodes();
	for (int i = 0; i < nodes.getLength(); i++) {
	    Node node = nodes.item(i);
	    if (!(node instanceof Element)) {
		continue;
	    }
	    Element element = (Element) node;
	    if (CONF_DATA.equals(element.getTagName())) {
		DataDesc data = buildDataDesc(element);
		if (data != null) {
		    datas.put(data.getName(), data);
		}
	    }
	}
    }

    /**
     * 
     * @return
     */
    public static final Map<String, DataDesc> getAll() {
	return datas;
    }
    
    /**
     * 
     * @return
     */
    public static final Collection<DataDesc> getAllTables(){
	return datas.values();
    }

    /**
     * 
     * @param table
     * @return
     */
    public static final DataDesc getTableName(String table) {
	return datas.get(table);
    }
    
    public static final void saveOrUpdate(DataDesc data) throws Exception{
	datas.put(data.getName(), data);
	save();
    }

    /**
     * 
     * @param dataNode
     * @return
     */
    private static final DataDesc buildDataDesc(Element dataNode) {
	DataDesc data = new DataDesc();
	String name = dataNode.getAttribute("name");
	data.setName(name);
	String desc = dataNode.getAttribute("desc");
	data.setDescription(desc);
	NodeList nodes = dataNode.getChildNodes();
	for (int i = 0; i < nodes.getLength(); i++) {
	    Node node = nodes.item(i);
	    if (!(node instanceof Element)) {
		continue;
	    }
	    Element element = (Element) node;
	    String value = element.getTextContent();
	    String tagName = element.getTagName();
	    if (CONF_DATA_SPLIT.equals(tagName)) {
		data.setDatasplit(value);
	    } else if (CONF_FIELDS.equals(tagName)) {
		NodeList famNodes = element.getChildNodes();
		Map<String, FieldDesc> fields = new HashMap<String, FieldDesc>();
		int order = 0;
		for (int j = 0; j < famNodes.getLength(); j++) {
		    Node fieldNode = famNodes.item(j);
		    if (!(fieldNode instanceof Element)) {
			continue;
		    }
		    Element famElement = (Element) fieldNode;
		    if (CONF_FIELD.equals(famElement.getTagName())) {
			FieldDesc field = buildField(famElement,order++);
			if (field != null) {
			    fields.put(field.getName(), field);
			}
		    } else {
			LOG.warn("tagName = " + famElement.getTagName() + " is not support!");
		    }
		}
		data.setFields(fields);
	    } else if (CONF_ROWKEY.equals(tagName)) {
		RowKeyDesc rowkey = new RowKeyDesc();
		NodeList famNodes = element.getChildNodes();
		List<String> rowkeys = new ArrayList<String>();
		for (int j = 0; j < famNodes.getLength(); j++) {
		    Node famNode = famNodes.item(j);
		    if (!(famNode instanceof Element)) {
			continue;
		    }
		    Element famElement = (Element) famNode;
		    if (CONF_FIELD.equals(famElement.getTagName())) {
			rowkeys.add(famElement.getTextContent());
		    } else if (CONF_ROWSPLIT.equals(famElement.getTagName())) {
			rowkey.setRowsplit(famElement.getTextContent());
		    }else {
			LOG.warn("tagName = " + famElement.getTagName() + " is not support!");
		    }
		}
		rowkey.setFields(rowkeys);
		data.setRowKey(rowkey);
	    } 
	}
	return data;
    }

    private static final FieldDesc buildField(Element fieldNode,int defOrder) {
	FieldDesc field = new FieldDesc();
	if(fieldNode.hasAttribute(CONF_FAMILY)){
	    String family = fieldNode.getAttribute(CONF_FAMILY);
	    field.setFamily(family);
	}
	
	if(fieldNode.hasAttribute(CONF_ORDER)){
	    String order = fieldNode.getAttribute(CONF_ORDER);
	    field.setOrder(StringUtil.toInt(order));
	}else{
	    field.setOrder(defOrder);
	}
	
	if(fieldNode.hasAttribute(CONF_DEF)){
	    String def = fieldNode.getAttribute(CONF_DEF);
	    field.setDef(def);
	}
	
	if(fieldNode.hasAttribute(CONF_DESC)){
	    String desc = fieldNode.getAttribute(CONF_DESC);
	    field.setDescription(desc);
	}
	
	
	if(fieldNode.hasAttribute(CONF_HAS_CHILD)){
	    String hasChild = fieldNode.getAttribute(CONF_HAS_CHILD);
	    if(StringUtil.toBoolean(hasChild)){
		NodeList nodes = fieldNode.getChildNodes();
		for (int i = 0; i < nodes.getLength(); i++) {
		    Node node = nodes.item(i);
		    if (!(node instanceof Element)) {
			continue;
		    }
		    Element fieldElement = (Element) node;
		    if (CONF_FIELDS.equals(fieldElement.getTagName())) {
			NodeList childNodes = fieldElement.getChildNodes();
			int order = 0;
			Map<String, FieldDesc> fields = new HashMap<String, FieldDesc>();
			for (int j = 0; j < childNodes.getLength(); j++) {
			    Node childNode = childNodes.item(j);
			    if (!(childNode instanceof Element)) {
				continue;
			    }
			    Element childElement = (Element) childNode;
			    if (CONF_FIELD.equals(childElement.getTagName())) {
				FieldDesc childField = buildField(childElement,order++);
				if (childField != null) {
				    fields.put(childField.getName(), childField);
				}
			    } else {
				LOG.warn("tagName = " + childElement.getTagName() + " is not support!");
			    }
			}
			field.setChilds(fields);
		    }else if(CONF_DATA_SPLIT.equalsIgnoreCase(fieldElement.getTagName())){
			field.setChildSplit(node.getTextContent());
		    }
		}
	    }else{
		 String value = fieldNode.getTextContent();
		 field.setName(value);
	    }
	}else{
	    String value = fieldNode.getTextContent();
	    field.setName(value);
	}
	return field;
    }
    
    public static void writeXml(Writer out, Document doc) throws IOException {
	try {
	    DOMSource source = new DOMSource(doc);
	    StreamResult result = new StreamResult(out);
	    TransformerFactory transFactory = TransformerFactory.newInstance();
	    Transformer transformer = transFactory.newTransformer();
	    transformer.transform(source, result);
	} catch (TransformerException te) {
	    throw new IOException(te);
	}
    }

    /**
     * Write out the non-default properties in this configuration to the given
     * {@link OutputStream} using UTF-8 encoding.
     * 
     * @param out
     *            the output stream to write to.
     */
    public static void writeXml(OutputStream out, Document doc)
	    throws IOException {
	writeXml(new OutputStreamWriter(out, "UTF-8"), doc);
    }

    public static final void save() throws Exception {
	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	factory.setIgnoringComments(true);
	factory.setNamespaceAware(true);
	try {
	    factory.setXIncludeAware(true);
	} catch (UnsupportedOperationException e) {
	    e.printStackTrace();
	}
	DocumentBuilder builder = factory.newDocumentBuilder();
	Document doc = builder.newDocument();
	Element datasNode = doc.createElement(CONF_DATAS);
	doc.appendChild(datasNode);
	for(Entry<String, DataDesc> data : datas.entrySet()){
	    datasNode.appendChild(data.getValue().buildNode(doc));
	}
	URL url = DataUtil.class.getResource(CONF_RESOURCE);
	File file = new File(url.toURI());
	writeXml(new FileOutputStream(file), doc);
    }
    
    public static void main(String[] args) {
	Map<String,DataDesc> datas = DataUtil.getAll();
	for(Entry<String, DataDesc> entry : datas.entrySet()){
	    System.out.println("==================");
	    System.out.println("Key = " + entry.getKey());
	    System.out.println("Value = " + entry.getValue());
	}
    }
}
