package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.WebClient;
import org.cataractsoftware.datasponge.DataRecord;
import org.junit.Ignore;
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

    @Test
    @Ignore("set url to run")
    public void testSearch() throws Exception {
        String url = "someURL";

        Properties props = new Properties();
        props.setProperty("textsearchextractor.mode", "ALL");
        props.setProperty("textsearchextractor.ignorecase", "true");
        props.setProperty("textsearchextractor.searchstring", "HI");


        WebClient client = new WebClient();
        TextSearchExtractor extractor = new TextSearchExtractor();
        extractor.init(props);
        Collection<DataRecord> records = extractor.extractData(url, client.getPage(url));
        assertNotNull(records);
        assertTrue(records.size() > 0);
    }
}
