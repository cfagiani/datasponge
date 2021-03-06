package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.WebClient;
import org.cataractsoftware.datasponge.DataRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Christopher Fagiani
 */
@RunWith(JUnit4.class)
public class HyperlinkExtractorTest {

    private static final String URL = "http://www.bc.edu/schools/cas/cs/";
    private static final String NO_LINKURL = "https://code.jquery.com/jquery-2.1.1.js";


    private HyperlinkExtractor buildExtractor() {
        HyperlinkExtractor extractor = new HyperlinkExtractor();
        Properties props = new Properties();
        extractor.init(props);
        return extractor;
    }


    @Test
    public void testLinkExtraction() throws Exception {
        WebClient client = new WebClient();
        DataExtractor extractor = buildExtractor();
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(URL));
        assertNotNull("link extractor should return one result", records);
        assertTrue("Link extractor should return one result", records.size() == 1);
        for (DataRecord r : records) {
            assertTrue("Page should have contained links", r.getFieldCount() > 0);
        }
    }

    @Test
    public void testNoLinks() throws Exception {
        WebClient client = new WebClient();
        DataExtractor extractor = buildExtractor();
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(NO_LINKURL));
        assertNotNull("link extractor should return one result", records);
        assertTrue("link extractor should return one result", records.size() == 1);
        for (DataRecord r : records) {
            assertTrue("Page should have not contained links", r.getFieldCount() == 0);
        }
    }

}
