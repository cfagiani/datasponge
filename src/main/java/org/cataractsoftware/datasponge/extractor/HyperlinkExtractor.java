package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Simple example of a DataExtractor. It will simply add a field to the data record for every hyperlink on a page.
 *
 * @author Christopher Fagiani
 */
public class HyperlinkExtractor implements DataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(HyperlinkExtractor.class);
    public static final String RECORD_TYPE = "linklist";
    public static final String FIELD_PREFIX = "link";


    public Collection<DataRecord> extractData(String url, Page page) {
        DataRecord record = new DataRecord(url, RECORD_TYPE);
        try {
            if (page.isHtmlPage()) {
                List<HtmlAnchor> anchors = ((HtmlPage)page).getAnchors();
                if(anchors != null) {
                    int count =0;
                    for (HtmlAnchor anchor : anchors) {
                        record.setField(FIELD_PREFIX + count, anchor.getHrefAttribute());
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Could not parse page", e);
        }
        return Arrays.asList(record);
    }

    @Override
    public void init(Properties props) {
        //no initialization required by this executor
    }
}
