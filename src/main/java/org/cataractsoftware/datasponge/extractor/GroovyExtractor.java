package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.GroovyDataAdapter;

import java.util.Collection;

/**
 * this data extractor uses loads a groovy class from the file specified in groovyextractorclass property and uses it to parse the page
 *
 * @author Christopher Fagiani
 */
public class GroovyExtractor extends GroovyDataAdapter implements DataExtractor {

    private static final String PROP_NAME = "groovyextractorclass";


    public Collection<DataRecord> extractData(String url, Page page) {
        return (Collection<DataRecord>) getGroovyObject().invokeMethod("extractData", new Object[]{url, page});
    }

    @Override
    protected String getScriptPropertyName() {
        return PROP_NAME;
    }
}
