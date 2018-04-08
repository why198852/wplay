package com.wplay.core.util;

import org.apache.hadoop.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

class DomainSuffixesReader {
	void read(DomainSuffixes tldEntries, InputStream input) throws IOException {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			factory.setIgnoringComments(true);
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new InputSource(input));

			Element root = document.getDocumentElement();

			if ((root != null) && (root.getTagName().equals("domains"))) {
				Element tlds = (Element) root.getElementsByTagName("tlds")
						.item(0);
				Element suffixes = (Element) root.getElementsByTagName(
						"suffixes").item(0);

				readITLDs(tldEntries,
						(Element) tlds.getElementsByTagName("itlds").item(0));
				readGTLDs(tldEntries,
						(Element) tlds.getElementsByTagName("gtlds").item(0));
				readCCTLDs(tldEntries,
						(Element) tlds.getElementsByTagName("cctlds").item(0));

				readSuffixes(tldEntries, suffixes);
			} else {
				throw new IOException("xml file is not valid");
			}
		} catch (ParserConfigurationException ex) {
			System.out.println(StringUtils.stringifyException(ex));
			throw new IOException(ex.getMessage());
		} catch (SAXException ex) {
			System.out.println(StringUtils.stringifyException(ex));
			throw new IOException(ex.getMessage());
		}
	}

	void readITLDs(DomainSuffixes tldEntries, Element el) {
		NodeList children = el.getElementsByTagName("tld");
		for (int i = 0; i < children.getLength(); i++)
			tldEntries.addDomainSuffix(readGTLD((Element) children.item(i),
					TopLevelDomain.Type.INFRASTRUCTURE));
	}

	void readGTLDs(DomainSuffixes tldEntries, Element el) {
		NodeList children = el.getElementsByTagName("tld");
		for (int i = 0; i < children.getLength(); i++)
			tldEntries.addDomainSuffix(readGTLD((Element) children.item(i),
					TopLevelDomain.Type.GENERIC));
	}

	void readCCTLDs(DomainSuffixes tldEntries, Element el) throws IOException {
		NodeList children = el.getElementsByTagName("tld");
		for (int i = 0; i < children.getLength(); i++)
			tldEntries.addDomainSuffix(readCCTLD((Element) children.item(i)));
	}

	TopLevelDomain readGTLD(Element el, TopLevelDomain.Type type) {
		String domain = el.getAttribute("domain");
		DomainSuffix.Status status = readStatus(el);
		float boost = readBoost(el);
		return new TopLevelDomain(domain, type, status, boost);
	}

	TopLevelDomain readCCTLD(Element el) throws IOException {
		String domain = el.getAttribute("domain");
		DomainSuffix.Status status = readStatus(el);
		float boost = readBoost(el);
		String countryName = readCountryName(el);
		return new TopLevelDomain(domain, status, boost, countryName);
	}

	DomainSuffix.Status readStatus(Element el) {
		NodeList list = el.getElementsByTagName("status");
		if ((list == null) || (list.getLength() == 0))
			return DomainSuffix.DEFAULT_STATUS;
		return DomainSuffix.Status.valueOf(list.item(0).getFirstChild()
				.getNodeValue());
	}

	float readBoost(Element el) {
		NodeList list = el.getElementsByTagName("boost");
		if ((list == null) || (list.getLength() == 0))
			return 1.0F;
		return Float.parseFloat(list.item(0).getFirstChild().getNodeValue());
	}

	String readCountryName(Element el) throws IOException {
		NodeList list = el.getElementsByTagName("country");
		if ((list == null) || (list.getLength() == 0))
			throw new IOException("Country name should be given");
		return list.item(0).getNodeValue();
	}

	void readSuffixes(DomainSuffixes tldEntries, Element el) {
		NodeList children = el.getElementsByTagName("suffix");
		for (int i = 0; i < children.getLength(); i++)
			tldEntries.addDomainSuffix(readSuffix((Element) children.item(i)));
	}

	DomainSuffix readSuffix(Element el) {
		String domain = el.getAttribute("domain");
		DomainSuffix.Status status = readStatus(el);
		float boost = readBoost(el);
		return new DomainSuffix(domain, status, boost);
	}
}