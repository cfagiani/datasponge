package org.cataractsoftware.datasponge.enhancer;

import org.cataractsoftware.datasponge.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Data enhancer used to attempt to remove duplicates. This is not 100% effective as it only works locally
 * (can only detect duplicates for records that it has seen on a single host) and it can evict previously seen
 * records from the internal cache if memory becomes constrained.
 * <p/>
 * This enhancer will use the equals method on the DataRecords by default. Customization of the comparison
 * can be made by changing the implementation of equals for DataRecord and/or by setting the customDetector
 * property on the plugin config (in the job configuration) to the fully-qualified name of a DuplicateDetector instance.
 *
 * TODO: update to only cache SHA-256 hash of the record instead of the whole thing
 *
 * @author Christopher Fagiani
 */
public class DeduplicationEnhancer implements DataEnhancer, DuplicateDetector {

    protected static final String COMPARATOR_PROPERTY = "customDetector";
    protected DuplicateDetector detector;
    protected Set<SoftReference<DataRecord>> seenRecords = new HashSet<>();
    protected Logger logger = LoggerFactory.getLogger(DeduplicationEnhancer.class);

    @Override
    public DataRecord enhanceData(DataRecord record) {
        if (checkIfSeen(record)) {
            return null;
        } else {
            return record;
        }
    }

    /**
     * checks if the record has been seen before. The answer may not be 100% correct since "seen" records could have
     * been released by the JVM if under memory pressure.
     * <p/>
     * If the item has not been seen, it is added to the cache.
     *
     * @param rec
     * @return
     */
    protected synchronized boolean checkIfSeen(DataRecord rec) {
        Set<SoftReference> expiredReferences = new HashSet<>();
        for (SoftReference<DataRecord> ref : seenRecords) {
            DataRecord r = ref.get();
            if (r != null) {
                if (detector.isDuplicate(r, rec)) {
                    return true;
                }
            } else {
                expiredReferences.add(ref);
            }
        }
        seenRecords.add(new SoftReference<>(rec));
        //do a little cleanup
        if (expiredReferences.size() > 0) {
            seenRecords.removeAll(expiredReferences);
        }
        return false;
    }


    /**
     * initializes this instance by constructing the comparator to be used in the duplicate detection
     *
     * @param props initialized property object containing all properties used to load the program
     */
    @Override
    public void init(Properties props) {
        detector = this;
        if (props != null) {
            String comparatorClass = props.getProperty(COMPARATOR_PROPERTY);
            if (comparatorClass != null && !comparatorClass.trim().isEmpty()) {
                try {
                    detector = (DuplicateDetector) Class.forName(comparatorClass).newInstance();
                } catch (Exception e) {
                    logger.error("Could not instantiate comparator. Using default.", e);
                }
            }
        }
    }


    @Override
    public void setJobId(String jobId) {

    }

    /**
     * default duplicate detection just uses "equals"
     *
     * @param r1
     * @param r2
     * @return
     */
    @Override
    public boolean isDuplicate(DataRecord r1, DataRecord r2) {
        if (r1 == null && r2 == null) {
            return true;
        } else if (r1 != null) {
            return r1.equals(r2);
        } else {
            return false;
        }
    }
}
