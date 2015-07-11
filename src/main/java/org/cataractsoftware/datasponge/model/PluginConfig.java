package org.cataractsoftware.datasponge.model;

import java.util.Properties;

/**
 * data structure representing the configuration of a pluggable module
 *
 * @author Christopher Fagiani
 */
public class PluginConfig {

    private Type type;
    private String className;
    private Properties pluginProperties;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Properties getPluginProperties() {
        return pluginProperties;
    }

    public void setPluginProperties(Properties pluginProperties) {
        this.pluginProperties = pluginProperties;
    }

    public enum Type {
        DATA_EXTRACTOR, DATA_WRITER, DATA_ENHANCER
    }


}
