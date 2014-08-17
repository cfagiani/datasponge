package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.SgmlPage;
import com.gargoylesoftware.htmlunit.TextPage;
import org.cataractsoftware.datasponge.DataRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * extractor that only outputs a DataRecord if the page being processed contains a search string.
 *
 * @author Christopher Fagiani
 */
public class TextSearchExtractor implements DataExtractor {

    private static final String MODE_PROPERTY_KEY = "textsearchextractor.mode";
    private static final String PREFIX_LENGTH_KEY = "textsearchextractor.prefixlength";
    private static final String SUFFIX_LENGTH_KEY = "textsearchextractor.suffixlength";
    private static final String SEARCHSTRING_KEY = "textsearchextractor.searchstring";
    private static final String IGNORE_CASE_KEY = "textsearchextractor.ignorecase";

    private static final String DEFAULT_PREFIX = "10";
    private static final String DEFAULT_SUFFIX = "10";

    public static final String RECORD_TYPE = "TextMatch";
    public static final String MATCH_TEXT_FIELD = "matchText";

    private int prefixLength;
    private int suffixLength;
    private boolean ignoreCase = true;

    private Mode mode = Mode.FULLTEXT;
    private String searchString;

    public enum Mode {FULLTEXT, FIRST, LAST, ALL}

    @Override
    public Collection<DataRecord> extractData(String url, Page page) {
        String body = null;
        if (page instanceof SgmlPage) {
            body = ((SgmlPage) page).asText();
        } else if (page instanceof TextPage) {
            body = ((TextPage) page).getContent();
        }

        return performSearch(url, body);
    }

    private Collection<DataRecord> performSearch(String url, String body) {
        Collection<DataRecord> records = new ArrayList<DataRecord>();
        String searchableText = body;
        if (ignoreCase) {
            searchableText = body.toUpperCase();
        }
        switch (mode) {
            case FULLTEXT:
                if (searchableText.contains(searchString)) {
                    DataRecord record = new DataRecord(url, RECORD_TYPE);
                    record.setField(MATCH_TEXT_FIELD, body);
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
        return records;
    }

    private DataRecord extractMatchText(String url, String text, int matchIdx) {
        if (matchIdx >= 0) {
            String contextText = text.substring(matchIdx - prefixLength >= 0 ? matchIdx - prefixLength : 0, matchIdx + suffixLength < text.length() - 1 ? matchIdx + suffixLength : text.length() - 1);
            DataRecord record = new DataRecord(url, RECORD_TYPE);
            record.setField(MATCH_TEXT_FIELD, contextText);
            return record;
        } else {
            return null;
        }
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
        if (ignoreCase) {
            searchString = searchString.toUpperCase();
        }
    }
}
