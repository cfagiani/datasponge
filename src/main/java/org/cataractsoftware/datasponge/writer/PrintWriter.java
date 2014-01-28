package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.DataRecord;

import java.util.Map;
import java.util.Set;

/**
 * Simple example of a DataWriter. This simply prints a String containing the id and field list for each DataRecord.
 *
 * @author Christopher Fagiani
 */
public class PrintWriter extends AbstractDataWriter {
    private static final String DELIMITER = "\t";

    @Override
    @SuppressWarnings("unchecked")
    protected void writeItem(DataRecord record) {
        String delimiter = getDelimiter();
        StringBuilder builder = new StringBuilder(record.getIdentifier());
        Set<Map.Entry<String, Object>> fields = record.getFields();
        if (fields != null) {
            for (Map.Entry<String, Object> entry : fields) {
                builder.append(delimiter).append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        System.out.println(builder.toString());
    }

    public void finish() {
        //no-op
    }

    protected String getDelimiter() {
        return DELIMITER;
    }
}
