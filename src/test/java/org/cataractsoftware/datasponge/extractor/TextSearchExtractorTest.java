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
public class TextSearchExtractorTest {

    private static final String URL = "https://code.jquery.com/jquery-2.1.1.js";
    private static final String FOUND_WORD = "window";
    private static final String NOT_FOUND_WORD = "Abandon all hope, ye who enter here";


    private TextSearchExtractor buildExtractor(String mode, String searchString) {
        TextSearchExtractor extractor = new TextSearchExtractor();
        Properties props = new Properties();
        props.setProperty("textsearchextractor.mode", mode);
        props.setProperty("textsearchextractor.ignorecase", "true");
        props.setProperty("textsearchextractor.searchstring", searchString);
        extractor.init(props);
        return extractor;
    }

    @Test
    public void testSearch() throws Exception {

        WebClient client = new WebClient();

        DataExtractor extractor = buildExtractor("ALL", FOUND_WORD);
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(URL));
        assertNotNull("extractor should never return null", records);
        assertTrue("should have found multiple results", records.size() > 1);
    }

    @Test
    public void testNotFound() throws Exception {

        WebClient client = new WebClient();

        DataExtractor extractor = buildExtractor("ALL", NOT_FOUND_WORD);
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(URL));
        assertNotNull("extractor should never return null", records);
        assertTrue("string should not have been found", records.size() == 0);
    }

    @Test
    public void testSingleMode() throws Exception {
        String url = "https://code.jquery.com/jquery-2.1.1.js";
        WebClient client = new WebClient();

        DataExtractor extractor = buildExtractor("FIRST", FOUND_WORD);
        Collection<DataRecord> records = extractor.extractData(url, client.getPage(url));
        assertNotNull("extractor should never return null", records);
        assertTrue("only one result should be returned", records.size() == 1);
    }


}
