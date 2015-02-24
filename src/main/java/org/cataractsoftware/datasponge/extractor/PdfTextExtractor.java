package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.PdfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * extracts the textual content of a PDF document and stores it in a data record with a single field named "text". For most practical
 * uses, users should subclass this class and override the processText method to suit the needs of the application.
 * @author Christopher Fagiani
 */
public class PdfTextExtractor implements DataExtractor {
    private Logger logger = LoggerFactory.getLogger(PdfTextExtractor.class);

    /**
     * extracts text data from a PDF and returns a set of DataRecords. This method will read the document into a single string and call the processText method to build the DataRecords.
     * @param url  url of page being processed
     * @param page page to process
     * @return
     */
    @Override
    public Collection<DataRecord> extractData(String url, Page page) {

        try {
            return processText(url, PdfUtil.extractTextFromPdf(url));
        } catch (IOException e) {
            logger.error("Could not read pdf from " + url, e);
        }
        return null;
    }


    /**
     * Builds a single DataRecord with a field called "text" containing the entire document. This method can be overridden to provide
     * application-specific parsing of PDFs.
     * @param url
     * @param allText
     * @return
     */
    protected Collection<DataRecord> processText(String url, String allText) {
        Collection<DataRecord> records = new ArrayList<DataRecord>();
        DataRecord record = new DataRecord(url, "pdf");
        record.setField("text", allText);
        records.add(record);
        return records;
    }

    @Override
    public void init(Properties props) {

    }
}
