package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataAdapter;
import org.cataractsoftware.datasponge.DataRecord;

/**
 * enhances a DataRecord with additional information, possibly from another source
 *
 * @author Christopher Fagiani
 */
public interface DataEnhancer extends DataAdapter {

    /**
     * enhances the data record passed in with additional information.
     *
     * @param record - record to enhance
     * @return - enhanced record
     */
    public DataRecord enhanceData(DataRecord record);
}
