package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import org.cataractsoftware.datasponge.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author Christopher Fagiani
 */
public class DirectoryExtractor implements DataExtractor {
    private Logger logger = LoggerFactory.getLogger(DirectoryExtractor.class);
    public static String PROTOCOL = "file://";

    public static final String DIR_RECORD_TYPE = "directoryListing";
    public static final String FILE_RECORD_TYPE = "file";
    public static final String FIELD_PREFIX = "dirEntry";
    public static final String SPECIAL_FILE_TYPE = "specialFile";


    @Override
    public Collection<DataRecord> extractData(String url, Page page) {
        //this is only applicable if current page is a directory
        File file = new File(url.replace(PROTOCOL, "").replace("/", File.separator));
        DataRecord record = new DataRecord(url, DIR_RECORD_TYPE);
        try {
            if (file.isDirectory()) {
                String[] contents = file.list();
                if (contents != null) {
                    for (int i = 0; i < contents.length; i++) {
                        record.setField(FIELD_PREFIX + i, contents[i]);
                    }
                }
            } else if(file.isFile()){
                record = new DataRecord(url, FILE_RECORD_TYPE);
            } else {
                record = new DataRecord(url, SPECIAL_FILE_TYPE);
            }
        } catch (Exception e) {
            logger.error("Could not perform directory listing", e);
        }
        return Arrays.asList(record);
    }

    @Override
    public void init(Properties props) {
        //no initialization needed
    }
}
