package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataRecord;

/**
 * interface for classes that can detect if two records are duplicates.
 *
 * @author Christopher Fagiani
 */
public interface DuplicateDetector {
    /**
     * returns true if r1 is a duplicate of r2. This may not be the same as "equals" (depends, of course, on the implementation).
     *
     * @param r1
     * @param r2
     * @return
     */
    boolean isDuplicate(DataRecord r1, DataRecord r2);
}
