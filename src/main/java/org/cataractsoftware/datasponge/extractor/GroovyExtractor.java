package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.GroovyDataAdapter;

/**
 * this data extractor uses loads a groovy class from the file specified in groovyextractorclass property and uses it to parse the page
 *
 * @author Christopher Fagiani
 */
public class GroovyExtractor extends GroovyDataAdapter implements DataExtractor {

    private static final String PROP_NAME = "groovyextractorclass";


    public DataRecord extractData(String url, HtmlPage page) {
        return (DataRecord) getGroovyObject().invokeMethod("extractData", new Object[]{url, page});
    }

    @Override
    protected String getScriptPropertyName() {
        return PROP_NAME;
    }
}
