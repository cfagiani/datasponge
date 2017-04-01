package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.AbstractDataAdapter;
import org.cataractsoftware.datasponge.DataRecord;

import java.util.HashSet;
import java.util.Properties;

/**
 * Base class for data writers. Most custom DataWriter implementations should extend this class. It handles keeping a
 * thread-safe collection of DataRecords that have yet to be written and
 * will flush them whenever the flushBatch method is run.
 *
 * @author Christopher Fagiani
 */
public abstract class AbstractDataWriter extends AbstractDataAdapter implements DataWriter {

    private final HashSet<DataRecord> dataRecordSet = new HashSet<DataRecord>();


    /**
     * initializes the internal collection of pending work
     *
     * @param props property object will all properties used to run the app
     */
    public void init(Properties props) {

    }

    /**
     * adds a DataRecord to the internal collection of items to be written
     * This method is thread-safe.
     *
     * @param record DataRecord instance to add to write list
     */
    public void addItem(DataRecord record) {
        synchronized (dataRecordSet) {
            dataRecordSet.add(record);
        }
    }

    /**
     * flushes the contents of the internal storage to disk and clears out the
     * internal store. This method is thread-safe.
     */
    @SuppressWarnings("unchecked")
    public void flushBatch() {
        HashSet<DataRecord> items = null;
        synchronized (dataRecordSet) {
            items = (HashSet<DataRecord>) dataRecordSet.clone();
            dataRecordSet.clear();
        }
        if (items != null) {
            startBatch();
            for (DataRecord item : items) {
                writeItem(item);
            }
            completeBatch();
        }
    }

    /**
     * this method will be called for each item in a batch when flushBatch is called.
     * Most implementations should write that item to the output
     *
     * @param record record instance to write
     */
    protected abstract void writeItem(DataRecord record);


    /**
     * lifecycle method called just before starting a batch (before the first call to writeItem).
     * Override this method if any set-up is required prior to writing a batch.
     */
    protected void startBatch() {
        //no-op
    }


    /**
     * lifecycle method called just after finishing a batch (after the last call to writeItem).
     * Override this method if any clean-up/commit logic needs to be performed for a batch
     */
    protected void completeBatch() {
        //no-op
    }
}
