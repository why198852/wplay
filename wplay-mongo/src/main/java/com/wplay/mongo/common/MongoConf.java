package com.wplay.mongo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *  @author James
 */
public class MongoConf {

    private static final Logger logger = LoggerFactory.getLogger(MongoConf.class);

    private static Map conf;

    static {
        InputStream inputStream = MongoConf.class.getClassLoader().getResourceAsStream("mongodb.properties");
        Properties properties = new Properties();
        conf = new HashMap();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        conf.putAll(properties);
    }

    public static synchronized Map getConf() throws ConfigurationException {
        if (null != conf) {
            return conf;
        } else {
            throw new ConfigurationException("MongConf parameter setting is not correct");
        }
    }

}
