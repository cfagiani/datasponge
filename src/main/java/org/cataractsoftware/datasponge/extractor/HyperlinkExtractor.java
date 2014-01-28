package org.cataractsoftware.datasponge.extractor;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Properties;

/**
 * Simple example of a DataExtractor. It will simply add a field to the data record for every hyperlink on a page.
 *
 * @author Christopher Fagiani
 */
public class HyperlinkExtractor implements DataExtractor<String> {

    private static final Logger logger = LoggerFactory.getLogger(HyperlinkExtractor.class);

    public DataRecord<String> extractData(String url, HtmlPage page) {
        DataRecord<String> record = new DataRecord<String>(url);
        try {
            NodeList nl = page.getElementsByTagName("a");
            if (nl != null) {
                for (int i = 0; i < nl.getLength(); i++) {
                    Node node = nl.item(i).getAttributes().getNamedItem(
                            "href");
                    if (node != null) {
                        record.setField("link" + i, node.getNodeValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Could not parse page", e);
        }
        return record;
    }

    @Override
    public void init(Properties props) {
        //no initialization required by this executor
    }
}
