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


    private TextSearchExtractor buildExtractor(String mode) {
        TextSearchExtractor extractor = new TextSearchExtractor();
        Properties props = new Properties();
        props.setProperty("textsearchextractor.mode", mode);
        props.setProperty("textsearchextractor.ignorecase", "true");
        props.setProperty("textsearchextractor.searchstring", "window");
        extractor.init(props);
        return extractor;
    }

    @Test
    public void testSearch() throws Exception {

        WebClient client = new WebClient();

        DataExtractor extractor = buildExtractor("ALL");
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(URL));
        assertNotNull(records);
        assertTrue(records.size() > 1);
    }

    @Test
    public void testSingleMode() throws Exception {
        String url = "https://code.jquery.com/jquery-2.1.1.js";
        WebClient client = new WebClient();

        DataExtractor extractor = buildExtractor("FIRST");
        Collection<DataRecord> records = extractor.extractData(url, client.getPage(url));
        assertNotNull(records);
        assertTrue(records.size() == 1);
    }


}
