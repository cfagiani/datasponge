package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import org.cataractsoftware.datasponge.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * this data extractor uses loads a groovy class from the file specified in groovyextractorclass property and uses it to parse the page
 *
 * @author Christopher Fagiani
 */
public class GroovyExtractor implements DataExtractor<String> {

    private static final Logger logger = LoggerFactory.getLogger(GroovyExtractor.class);
    private static final String PROP_NAME = "groovyextractorclass";

    private GroovyObject groovyObject;


    public DataRecord<String> extractData(String url, HtmlPage page) {
        return (DataRecord) groovyObject.invokeMethod("extractData", new Object[]{url, page});
    }

    @Override
    public void init(Properties props) {

        String val = props.getProperty(PROP_NAME);
        if (val == null || val.trim().isEmpty()) {
            throw new IllegalStateException("GroovyExtractor used but no " + PROP_NAME + " specified in property file");
        }
        try {
            ClassLoader parent = getClass().getClassLoader();
            GroovyClassLoader loader = new GroovyClassLoader(parent);
            File classFile = new File(val);
            Class groovyClass = loader.parseClass(classFile);
            groovyObject = (GroovyObject) groovyClass.newInstance();

        } catch (Exception e) {
            logger.error("Could not load groovy file", e);
            throw new IllegalStateException("Could not initialize the GroovyExtractor.", e);
        }
    }
}
