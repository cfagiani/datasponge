package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.DataAdapter;
import org.cataractsoftware.datasponge.DataRecord;

/**
 * interface defining the methods for DataWriters. Implementors are responsible for transforming a DataRecord into the desired format and writing it
 * to whatever output is required.
 *
 * @author Christopher Fagiani
 */
public interface DataWriter extends DataAdapter {

    /**
     * add an item to the list of pending work to be written.
     *
     * @param record DataRecord to add
     */
    public void addItem(DataRecord record);

    /**
     * flush the list of work to be written
     */
    public void flushBatch();

    /**
     * called by the system just prior to a normal shutdown.
     */
    public void finish();


}
