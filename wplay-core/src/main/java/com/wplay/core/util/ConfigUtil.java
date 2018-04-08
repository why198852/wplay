package com.wplay.core.util;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class ConfigUtil {

    private static Parameters params = new Parameters() ;

    public static ImmutableConfiguration getConfiguration(String filename) throws ConfigurationException {
        FileBasedConfigurationBuilder builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class).configure(params.properties().setFileName(filename)) ;
        return builder.getConfiguration();
    }

}
