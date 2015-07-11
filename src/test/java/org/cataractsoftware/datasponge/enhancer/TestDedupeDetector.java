package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataRecord;


/**
 * simple custom comparator to test the behavior of the DeduplicationEnhancer. This only checks the ID. It should NOT be
 * used for anything other than the test case.
 *
 * @author Christopher Fagiani
 */
public class TestDedupeDetector implements DuplicateDetector {
    @Override
    public boolean isDuplicate(DataRecord o1, DataRecord o2) {
        if (o1.getIdentifier().equals(o2.getIdentifier())) return true;
        else return false;
    }
}
