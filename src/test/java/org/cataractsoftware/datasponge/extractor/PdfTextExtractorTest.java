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
public class PdfTextExtractorTest {
    private static final String URL = "http://www2.erie.gov/comptroller/sites/www2.erie.gov.comptroller/files/uploads/2015%20Vendor%20Checks%20-%20Week%20Ending%2002-20-15.pdf";

    private PdfTextExtractor buildExtractor() {
        PdfTextExtractor extractor = new PdfTextExtractor();
        Properties props = new Properties();
        extractor.init(props);
        return extractor;
    }


    @Test
    public void testExtraction() throws Exception {
        WebClient client = new WebClient();
        DataExtractor extractor = buildExtractor();
        Collection<DataRecord> records = extractor.extractData(URL, client.getPage(URL));
        assertNotNull("pdf extractor should return one result", records);
        assertTrue("pdf extractor should return one result", records.size() == 1);
        for (DataRecord r : records) {
            assertTrue("Page should have contained text", r.getFieldCount() > 0);
        }
    }
}
