package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.GroovyDataAdapter;


/**
 * data enhancer that relies on a groovy script to perform the enhancement. The location of the script file
 * must be specified in the properties file under the key "groovyenhancerclass"
 * @author Christopher Fagiani
 */
public class GroovyEnhancer extends GroovyDataAdapter implements DataEnhancer {

    private static final String PROP_NAME = "groovyenhancerclass";


    public DataRecord enhanceData(DataRecord record) {
        return (DataRecord) getGroovyObject().invokeMethod("enhanceData", record);
    }


    @Override
    protected String getScriptPropertyName() {
        return PROP_NAME;
    }

}
