package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.DataRecord;

import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * DataWriter that can be used to dump data to a CSV
 * <p/>
 * The behavior of this writer can be configured via the following properties:
 * <p/>
 * csvwriter.filename = the file to write (fully qualified path)
 * csvwriter.mode = either "append" or "overwrite"
 * csvwriter.includeheader = if true, it will write a header row to the file prior to writing data
 * csvwriter.fieldorder = semicolon-delimited list of field names to write
 *
 * @author Christopher Fagiani
 */
public class CSVFileWriter extends AbstractDataWriter {

    private static final String FILENAME = "csvwriter.filename";
    private static final String MODE = "csvwriter.mode";
    private static final String HEADER_FLAG = "csvwriter.includeheader";
    private static final String FIELDS = "csvwriter.fieldorder";
    private static final String DELIM = "csvwriter.delimiter";

    private static final String APPEND = "append";

    private List<String> fieldsToWrite;
    private FileWriter writer;

    private String delimiter;

    @Override
    public void init(Properties props) {
        super.init(props);
        String fileName = props.getProperty(FILENAME);
        boolean append = APPEND.equalsIgnoreCase(props.getProperty(MODE, "overwrite"));
        String headerString = props.getProperty(FIELDS);
        delimiter = props.getProperty(DELIM, ",");
        if (headerString == null || headerString.trim().length() == 0) {
            throw new IllegalStateException(FIELDS + " must be non-empty in the property file when using CSVFileWriter");
        }
        fieldsToWrite = Arrays.asList(headerString.split(";"));
        try {
            writer = new FileWriter(fileName, append);
            if (Boolean.parseBoolean(props.getProperty(HEADER_FLAG, "false"))) {
                writer.write(headerString.replaceAll(";", delimiter) + "\n");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not configure CSVFileWriter", e);
        }
    }

    @Override
    protected void writeItem(DataRecord record) {
        StringBuilder builder = new StringBuilder();
        for (String field : fieldsToWrite) {
            if (builder.length() > 0) {
                builder.append(delimiter);
            }
            builder.append(record.getFieldValue(field));
        }
        builder.append("\n");
        try {
            writer.write(builder.toString());
        } catch (Exception e) {
            throw new RuntimeException("Could not write item to file", e);
        }
    }

    @Override
    public void finish() {
        try {
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException("Could not close output file", e);
        }
    }

    @Override
    protected void completeBatch() {
        super.completeBatch();
        try {
            writer.flush();
        } catch (Exception e) {
            throw new RuntimeException("could not flush batch to disk", e);
        }
    }
}
