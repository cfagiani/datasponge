package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * test case for deduplication enhancer
 *
 * @author Christopher Fagiani
 */
@RunWith(JUnit4.class)
public class DeduplicationEnhancerTest {
    private static final String REC_TYPE = "TEST";

    @Test
    public void testDuplicateDefault() {

        DataRecord rec = TestUtils.buildRecord("1", REC_TYPE);
        DataEnhancer enhancer = buildEnhancer(null);
        rec = enhancer.enhanceData(rec);
        assertNotNull("Record should not be null", rec);
        rec = TestUtils.buildRecord("2", REC_TYPE);
        rec = enhancer.enhanceData(rec);
        assertNotNull("Record should not be null", rec);
        rec = TestUtils.buildRecord("2", REC_TYPE);
        rec = enhancer.enhanceData(rec);
        assertNull("Record should be null since it's a duplicate", rec);
        rec = TestUtils.buildRecord("2", "BLAH");
        rec = enhancer.enhanceData(rec);
        assertNotNull("Record should not be null since it's of a different type", rec);
    }


    @Test
    public void testDuplicateCustom() {

        DataRecord rec = TestUtils.buildRecord("1", REC_TYPE);
        DataEnhancer enhancer = buildEnhancer(TestDedupeDetector.class.getCanonicalName());
        rec = enhancer.enhanceData(rec);
        assertNotNull("Record should not be null", rec);
        rec = TestUtils.buildRecord("2", REC_TYPE);
        rec = enhancer.enhanceData(rec);
        assertNotNull("Record should not be null", rec);
        rec = TestUtils.buildRecord("2", REC_TYPE);
        rec = enhancer.enhanceData(rec);
        assertNull("Record should be null since it's a duplicate", rec);
        rec = TestUtils.buildRecord("2", "BLAH");
        rec = enhancer.enhanceData(rec);
        assertNull("Record should be null even though it's of a different type", rec);
    }


    private DataEnhancer buildEnhancer(String comparatorClass) {
        DeduplicationEnhancer enhancer = new DeduplicationEnhancer();
        Properties props = new Properties();
        if (comparatorClass != null) {
            props.put(DeduplicationEnhancer.COMPARATOR_PROPERTY, comparatorClass);
        }
        enhancer.init(props);
        return enhancer;
    }

}
