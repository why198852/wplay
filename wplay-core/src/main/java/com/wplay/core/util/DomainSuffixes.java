package com.wplay.core.util;

import org.apache.hadoop.util.StringUtils;

import java.io.InputStream;
import java.util.HashMap;

public class DomainSuffixes {
	private HashMap<String, DomainSuffix> domains = new HashMap<String, DomainSuffix>();
	private static DomainSuffixes instance;

	private DomainSuffixes() {
		String file = "domain-suffixes.xml";
		InputStream input = getClass().getClassLoader().getResourceAsStream(file);
		try {
			new DomainSuffixesReader().read(this, input);
		} catch (Exception ex) {
			StringUtils.stringifyException(ex);
		}
	}

	public static DomainSuffixes getInstance() {
		if (instance == null) {
			instance = new DomainSuffixes();
		}
		return instance;
	}

	void addDomainSuffix(DomainSuffix tld) {
		this.domains.put(tld.getDomain(), tld);
	}

	public boolean isDomainSuffix(String extension) {
		return this.domains.containsKey(extension);
	}

	public DomainSuffix get(String extension) {
		return (DomainSuffix) this.domains.get(extension);
	}
}