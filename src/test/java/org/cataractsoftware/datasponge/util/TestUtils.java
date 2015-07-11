package org.cataractsoftware.datasponge.util;

import org.cataractsoftware.datasponge.DataRecord;

/**
 * utilty methods for use in tests
 *
 * @author Christopher Fagiani
 */
public class TestUtils {

    public static DataRecord buildRecord(String id, String type) {
        return new DataRecord(id, type);
    }
}
