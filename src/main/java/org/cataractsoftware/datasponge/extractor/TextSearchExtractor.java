package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.*;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.util.PdfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * extractor that only outputs a DataRecord if the page being processed contains a search string.
 *
 * @author Christopher Fagiani
 */
public class TextSearchExtractor implements DataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TextSearchExtractor.class);

    private static final String MODE_PROPERTY_KEY = "textsearchextractor.mode";
    private static final String PREFIX_LENGTH_KEY = "textsearchextractor.prefixlength";
    private static final String SUFFIX_LENGTH_KEY = "textsearchextractor.suffixlength";
    private static final String SEARCHSTRING_KEY = "textsearchextractor.searchstring";
    private static final String IGNORE_CASE_KEY = "textsearchextractor.ignorecase";
    private static final String SUPPORTED_FILETYPES = "textsearchextractor.supportedfiletypes";

    private static final String DEFAULT_PREFIX = "10";
    private static final String DEFAULT_SUFFIX = "10";

    public static final String RECORD_TYPE = "TextMatch";
    public static final String MATCH_TEXT_FIELD = "matchText";
    public static final String IDX_FIELD = "matchStartIdx";

    private int prefixLength;
    private int suffixLength;
    private boolean ignoreCase = true;

    private Mode mode = Mode.FULLTEXT;
    private String searchString;
    private List<String> supportedFiletypes;


    public enum Mode {FULLTEXT, FIRST, LAST, ALL}

    @Override
    public Collection<DataRecord> extractData(String url, Page page) {
        String body = null;
        if (url != null && url.toLowerCase().startsWith("file:")) {
            body = getBodyFromFile(url);
        } else if (page instanceof SgmlPage) {
            body = ((SgmlPage) page).asText();
        } else if (page instanceof TextPage) {
            body = ((TextPage) page).getContent();
        } else if (page instanceof JavaScriptPage) {
            body = ((JavaScriptPage) page).getContent();
        }else if (page instanceof UnexpectedPage || page instanceof BinaryPage){
            if(url.toLowerCase().trim().endsWith(".pdf")){
                try {
                    body = PdfUtil.extractTextFromPdf(url);
                }catch(IOException e){
                    logger.error("Could not extract PDF",e);
                }
            }
        }
        return performSearch(url, body);
    }

    /**
     * reads a file's content into a string
     * TODO: handle non-text content (XLS(X), PPT(X), etc)
     *
     * @param fileUrl
     * @return
     */
    private String getBodyFromFile(String fileUrl) {
        File file = new File(fileUrl.replace("file://", "").replace("/", File.separator));
        if ( file.exists() && file.isFile() && shouldSearch(file.getName())){
            if(fileUrl.toLowerCase().endsWith(".pdf")){
                try {
                    return PdfUtil.extractTextFromPdf(file);
                }catch(IOException e){
                    logger.error("Could not read body from pdf",e);
                }
            }else {
                BufferedReader reader = null;
                try {
                    StringBuilder builder = new StringBuilder();
                    reader = new BufferedReader(new FileReader(file));
                    String line = reader.readLine();
                    while (line != null) {
                        builder.append(line).append("\n");
                        line = reader.readLine();
                    }
                    return builder.toString();

                } catch (Exception e) {
                    logger.error("Could not read file", e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (Exception e) {
                            logger.error("Could not close reader", e);
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * searches the body based onthe mode
     *
     * @param url
     * @param body
     * @return
     */
    private Collection<DataRecord> performSearch(String url, String body) {
        Collection<DataRecord> records = new ArrayList<DataRecord>();
        if (body != null) {
            String searchableText = body;
            if (ignoreCase) {
                searchableText = body.toUpperCase();
            }
            switch (mode) {
                case FULLTEXT:
                    if (searchableText.contains(searchString)) {
                        DataRecord record = new DataRecord(url, RECORD_TYPE);
                        record.setField(MATCH_TEXT_FIELD, body);
                        record.setField(IDX_FIELD, searchableText.indexOf(searchString));
                        records.add(record);
                    }
                    break;
                case FIRST:
                    DataRecord firstRecord = extractMatchText(url, body, searchableText.indexOf(searchString));
                    if (firstRecord != null) {
                        records.add(firstRecord);
                    }
                    break;
                case LAST:
                    DataRecord lastRecord = extractMatchText(url, body, searchableText.lastIndexOf(searchString));
                    if (lastRecord != null) {
                        records.add(lastRecord);
                    }
                    break;
                case ALL:
                    int lastIdx = 0;
                    do {
                        lastIdx = searchableText.indexOf(searchString, lastIdx + 1);
                        if (lastIdx >= 0) {
                            records.add(extractMatchText(url + records.size(), body, lastIdx));
                        }
                    } while (lastIdx >= 0 && lastIdx < searchableText.length() - 2);
                    break;
                default:
                    break;
            }
        }
        return records;
    }

    /**
     * constructs a data record that contains the prefix + suffix characters around the match
     *
     * @param url
     * @param text
     * @param matchIdx
     * @return
     */
    private DataRecord extractMatchText(String url, String text, int matchIdx) {
        if (matchIdx >= 0) {
            String contextText = text.substring(matchIdx - prefixLength >= 0 ? matchIdx - prefixLength : 0, matchIdx + suffixLength < text.length() - 1 ? matchIdx + suffixLength : text.length() - 1);
            DataRecord record = new DataRecord(url, RECORD_TYPE);
            record.setField(MATCH_TEXT_FIELD, contextText);
            record.setField(IDX_FIELD, matchIdx);
            return record;
        } else {
            return null;
        }
    }

    /**
     * returns true if the file is covered by the supportedFiletypes property
     *
     * @param name
     * @return
     */
    private boolean shouldSearch(String name) {
        if (supportedFiletypes.size() > 0) {
            for (String type : supportedFiletypes) {
                if (name.toLowerCase().endsWith(type)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    @Override
    public void init(Properties props) {
        String modeProp = props.getProperty(MODE_PROPERTY_KEY);
        if (modeProp != null) {
            mode = Mode.valueOf(modeProp.toUpperCase());
        }
        prefixLength = Integer.parseInt(props.getProperty(PREFIX_LENGTH_KEY, DEFAULT_PREFIX));
        suffixLength = Integer.parseInt(props.getProperty(SUFFIX_LENGTH_KEY, DEFAULT_SUFFIX));
        ignoreCase = Boolean.parseBoolean(props.getProperty(IGNORE_CASE_KEY, "true"));
        searchString = props.getProperty(SEARCHSTRING_KEY);
        String fileTypeString = props.getProperty(SUPPORTED_FILETYPES, "");
        supportedFiletypes = Arrays.asList(fileTypeString.toLowerCase().split(","));
        if (ignoreCase) {
            searchString = searchString.toUpperCase();
        }
    }
}
