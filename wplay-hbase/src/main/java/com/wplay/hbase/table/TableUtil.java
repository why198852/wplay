package com.wplay.hbase.table;

import com.wplay.core.util.StringUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author user
 * 
 */
public class TableUtil {
    private static final Logger LOG = Logger.getLogger(TableUtil.class);
    private static final Map<String, TableInfo> tables = new HashMap<String, TableInfo>();

    static{
	try {
	    init();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
    
    public static final void init() throws Exception {
	tables.clear();
	init("/table-parse.xml");
    }

    public static final void init(String resource) throws SAXException,
	    IOException, ParserConfigurationException {
	InputStream in = TableUtil.class.getResourceAsStream(resource);
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
	if (!"tables".equals(root.getTagName())) {
	    LOG.error("bad conf file : top-level element not <tables>");
	}
	NodeList nodes = root.getChildNodes();
	for (int i = 0; i < nodes.getLength(); i++) {
	    Node node = nodes.item(i);
	    if (!(node instanceof Element)) {
		continue;
	    }
	    Element element = (Element) node;
	    if ("table".equals(element.getTagName())) {
		TableInfo table = buildTableInfo(element);
		if (table != null) {
		    tables.put(table.getName(), table);
		}
	    }
	}
    }

    /**
     * 
     * @return
     */
    public static final Map<String, TableInfo> getAll() {
	return tables;
    }
    
    /**
     * 
     * @return
     */
    public static final Collection<TableInfo> getAllTables(){
	return tables.values();
    }

    /**
     * 
     * @param table
     * @return
     */
    public static final TableInfo getTableName(String table) {
	return tables.get(table);
    }

    /**
     * 
     * @param tableNode
     * @return
     */
    private static final TableInfo buildTableInfo(Element tableNode) {
	TableInfo tableInfo = new TableInfo();
	String name = tableNode.getAttribute("name");
	tableInfo.setName(name);
	NodeList nodes = tableNode.getChildNodes();
	for (int i = 0; i < nodes.getLength(); i++) {
	    Node node = nodes.item(i);
	    if (!(node instanceof Element)) {
		continue;
	    }
	    Element element = (Element) node;
	    String value = element.getTextContent();
	    String tagName = element.getTagName();
	    if ("start".equals(tagName)) {
		tableInfo.setStart(value);
	    } else if ("stop".equals(tagName)) {
		tableInfo.setEnd(value);
	    } else if ("regions".equals(tagName)) {
		tableInfo.setRegions(Integer.parseInt(value));
	    } else if ("split".equals(tagName)) {
		if (StringUtil.isEmpty(value)) {
		    tableInfo.setSplit(true);
		} else {
		    tableInfo.setSplit(StringUtil.toBoolean(value));
		}
	    } else if ("familys".equals(tagName)) {
		NodeList famNodes = element.getChildNodes();
		for (int j = 0; j < famNodes.getLength(); j++) {
		    Node famNode = famNodes.item(j);
		    if (!(famNode instanceof Element)) {
			continue;
		    }
		    Element famElement = (Element) famNode;
		    if ("family".equals(famElement.getTagName())) {
			HBaseFamilyCloumn fam = buildFamily(famElement);
			if (fam != null) {
			    tableInfo.addFamily(fam);
			}
		    } else {
			LOG.warn("tagName = " + famElement.getTagName() + " is not support!");
		    }
		}
	    } else {
		LOG.warn("tagName = " + tagName + " is not support!");
	    }
	}
	return tableInfo;
    }

    private static final HBaseFamilyCloumn buildFamily(Element familyNode) {
	HBaseFamilyCloumn family = new HBaseFamilyCloumn();
	String name = familyNode.getAttribute("name");
	family.setFamilyName(name);
	NodeList nodes = familyNode.getChildNodes();
	for (int i = 0; i < nodes.getLength(); i++) {
	    Node node = nodes.item(i);
	    if (!(node instanceof Element)) {
		continue;
	    }
	    Element element = (Element) node;
	    String tagName = element.getTagName();
	    String value = element.getTextContent();
	    if ("inMemory".equalsIgnoreCase(tagName)) {
		if (!StringUtil.isEmpty(value)) {
		    family.setInMemory(StringUtil.toBoolean(value));
		}
	    } else if ("version".equalsIgnoreCase(tagName)) {
		if (!StringUtil.isEmpty(value)) {
		    family.setVersion(StringUtil.toInt(value));
		}
	    } else if ("compression".equalsIgnoreCase(tagName)) {
		if (!StringUtil.isEmpty(value)) {
		    family.setCompression(value);
		} else {
		    family.setCompression(Compression.Algorithm.GZ.toString());
		}
	    } else if("replication".equalsIgnoreCase(tagName)){
		if (!StringUtil.isEmpty(value)) {
		    family.setReplicationScope(StringUtil.toInt(value));
		}
	    }
	}
	return family;
    }
}
