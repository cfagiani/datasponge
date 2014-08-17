package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataAdapter;
import org.cataractsoftware.datasponge.DataRecord;

import java.util.Collection;

/**
 * interface that defines the methods for extracting data from a page.
 *
 * @author Christopher Fagiani
 */
public interface DataExtractor extends DataAdapter {

    /**
     * implementations should return a collection of DataRecord objects populated with data extracted from the page
     *
     * @param url  url of page being processed
     * @param page page to process
     * @return DataRecord object containing data extracted from the page
     */
    public Collection<DataRecord> extractData(String url, Page page);
}
