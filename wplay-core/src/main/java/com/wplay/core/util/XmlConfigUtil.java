package com.wplay.core.util;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
  *
 */
public class XmlConfigUtil {
    private static Configuration conf;
    public static final String UUID_KEY = "sky.conf.uuid";

    /**
     *
     * @param conf
     */
    private static void setUUID(Configuration conf) {
        UUID uuid = UUID.randomUUID();
        conf.set("sky.conf.uuid", uuid.toString());
    }

    /**
     *
     * @param conf
     * @return
     */
    public static String getUUID(Configuration conf) {
        return conf.get("sky.conf.uuid");
    }

    /**
     *
     * @return
     */
    public static Configuration create() {
        if(conf == null){
            conf = new Configuration();
            setUUID(conf);
            addSkyResources(conf);
            //JarsUtil.addTmpJars(conf);
        }
        return conf;
    }

    /**
     *
     * @param addSkyResources
     * @param skyProperties
     * @return
     */
    public static Configuration create(boolean addSkyResources,
                                       Properties skyProperties) {
        Configuration conf = new Configuration();
        setUUID(conf);
        if (addSkyResources) {
            addSkyResources(conf);
        }
        for (Map.Entry<Object, Object> e : skyProperties.entrySet()) {
            conf.set(e.getKey().toString(), e.getValue().toString());
        }
        return conf;
    }

    /**
     *
     * @param conf
     * @return
     */
    private static Configuration addSkyResources(Configuration conf) {
        conf.addResource("core-site.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
        return conf;
    }
}
