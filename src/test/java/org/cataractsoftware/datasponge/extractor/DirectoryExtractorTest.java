package org.cataractsoftware.datasponge.extractor;

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
public class DirectoryExtractorTest {

    private static final String DIR = "file:///Users/cfagiani";
    private static final String FILE = "file:///Users/cfagiani/test.txt";


    private DirectoryExtractor buildExtractor() {
        DirectoryExtractor extractor = new DirectoryExtractor();
        Properties props = new Properties();
        extractor.init(props);
        return extractor;
    }


    @Test
    public void testLinkExtraction() throws Exception {
        DataExtractor extractor = buildExtractor();
        Collection<DataRecord> records = extractor.extractData(DIR, null);
        assertNotNull("link extractor should return one result", records);
        assertTrue("Link extractor should return one result", records.size() == 1);
        for (DataRecord r : records) {
            assertTrue("Page should have contained links", r.getFieldCount() > 0);
        }
    }

    @Test
    public void testFile() throws Exception {
        DataExtractor extractor = buildExtractor();
        Collection<DataRecord> records = extractor.extractData(FILE, null);
        assertNotNull("link extractor should return one result", records);
        assertTrue("Link extractor should return one result", records.size() == 1);
    }
}
