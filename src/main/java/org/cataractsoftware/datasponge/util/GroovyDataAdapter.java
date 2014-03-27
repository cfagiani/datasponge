package org.cataractsoftware.datasponge.util;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import org.cataractsoftware.datasponge.DataAdapter;

import java.io.File;
import java.util.Properties;

/**
 * data adaptor base class that facilitates loading a Groovy script from a file
 *
 * @author Christopher Fagiani
 */
public abstract class GroovyDataAdapter implements DataAdapter {

    private GroovyObject groovyObject;

    /**
     * returns the name of the property key to use when loading the groovy script
     *
     * @return
     */
    abstract protected String getScriptPropertyName();

    /**
     * returns the groovy object initialized by the init method.
     *
     * @return
     */
    protected GroovyObject getGroovyObject() {
        return groovyObject;
    }


    @Override
    public void init(Properties props) {
        String val = props.getProperty(getScriptPropertyName());
        if (val == null || val.trim().isEmpty()) {
            throw new IllegalStateException("GroovyEnhancer used but no " + getScriptPropertyName() + " specified in property file");
        }
        try {
            ClassLoader parent = getClass().getClassLoader();
            GroovyClassLoader loader = new GroovyClassLoader(parent);


            File classFile = new File(val);
            Class groovyClass = loader.parseClass(classFile);
            groovyObject = (GroovyObject) groovyClass.newInstance();
            if(groovyObject.getMetaClass().respondsTo(groovyObject, "setProperties").size()>0){
                groovyObject.invokeMethod("setProperties",new Object[]{props});
            }

        } catch (Exception e) {
            throw new IllegalStateException("Could not initialize the GroovyEnhancer.", e);
        }
    }
}
