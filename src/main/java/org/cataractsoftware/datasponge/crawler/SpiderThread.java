package org.cataractsoftware.datasponge.crawler;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.extractor.DataExtractor;
import org.cataractsoftware.datasponge.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

/**
 * This class is meant to be run in its own thread. It will pop an url off the
 * top of the thread-safe workqueue object, load the page designated by that
 * url, extract the links from that page, add those links to the queue and then
 * extract the tags from the page. Each tag will be added to a shared collector
 * object (thread-safe) for output.
 *
 * @author Christopher Fagiani
 */
public class SpiderThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpiderThread.class);

    private CrawlerWorkqueue queue;
    private volatile boolean busy;

    private DataWriter outputCollector;
    private String proxy;
    private int port;

    public static final int MAX_IDLE_ITERATIONS = 2;
    public static final int BACKOFF_INTERVAL = 10000;
    private DataExtractor extractor;

    public boolean isBusy() {
        return busy;
    }

    /**
     * creates a new SpiderThread object that will add its output to the
     * collector object passed in. If proxy is not null, it will use the proxy
     * server in the proxy parameter for all http requests.
     *
     * @param proxy     proxy host
     * @param port      proxy port
     * @param collector initialized DataWriter instance
     */
    public SpiderThread(String proxy, int port, DataWriter collector,
                        DataExtractor extractor) {
        this.proxy = proxy;
        this.port = port;

        queue = CrawlerWorkqueue.getInstance();
        this.extractor = extractor;
        busy = true;
        outputCollector = collector;

    }

    /**
     * This method will instantiate a WebClient object (provided by the HtmlUnit
     * library)then do the following in a loop:<br>
     * <ul>
     * <li>pop an URL off the queue</li>
     * <li>load the web page</li>
     * <li>extract all hyperlinks and add them to the queue</li>
     * <li>extract all tags and add them to the collector</li>
     * </ul>
     * <br>
     * The loop terminates when it has executed MAX_IDLE_ITERATIONS times
     * without having any work to perform (i.e. while the queue is empty)
     * <p/>
     * This method will swallow exceptions so that errors with a single HTML
     * page do not cause the crawl to abort.
     */
    @Override
    public void run() {
        final WebClient webClient = initializeWebClient();
        int idleIterations = 0;

        while (busy) {
            try {
                String url = queue.dequeue();
                if (url != null) {
                    busy = true;
                    idleIterations = 0;

                    final HtmlPage page = webClient.getPage(url);

                    extractLinks(page, url);
                    DataRecord dr = extractor.extractData(url, page);
                    if (dr != null) {
                        outputCollector.addItem(dr);
                    }

                } else {
                    if (idleIterations > MAX_IDLE_ITERATIONS) {
                        busy = false;
                    } else {
                        Thread.sleep(BACKOFF_INTERVAL);
                        idleIterations++;
                    }
                }
            } catch (FailingHttpStatusCodeException e) {
                logger.error("HTTP Failure", e);
            } catch (MalformedURLException e) {
                logger.error("Bad URL", e);
            } catch (IOException e) {
                logger.error("IO Error", e);
            } catch (InterruptedException e) {
                logger.error("Thread interrupt", e);
            }
        }
    }

    /**
     * extracts all hyperlinks from a page
     *
     * @param page     page to process
     * @param thisPage url of current page
     */
    private void extractLinks(HtmlPage page, String thisPage) {
        List<HtmlAnchor> anchors = page.getAnchors();
        for (HtmlAnchor anchor : anchors) {
            String link = anchor.getHrefAttribute();
            if (link != null && !link.toLowerCase().startsWith("mailto:")) {
                queue.enqueue(link, thisPage);
            }
        }
    }


    /**
     * initializes the WebClient object that will be used to fetch and parse web
     * pages
     *
     * @return new instance of WebClient that can be used to load and parse pages
     */
    private WebClient initializeWebClient() {

        if (proxy != null) {
            return new WebClient(BrowserVersion.CHROME,
                    proxy, port);
        } else {
            return new WebClient(BrowserVersion.CHROME);
        }
    }
}