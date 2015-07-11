package org.cataractsoftware.datasponge.crawler;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.WebClient;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.enhancer.DataEnhancer;
import org.cataractsoftware.datasponge.extractor.DataExtractor;
import org.cataractsoftware.datasponge.extractor.DirectoryExtractor;
import org.cataractsoftware.datasponge.extractor.HyperlinkExtractor;
import org.cataractsoftware.datasponge.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Map;

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

    public static final int MAX_IDLE_ITERATIONS = 2;
    public static final int BACKOFF_INTERVAL = 10000;
    private static final Logger logger = LoggerFactory
            .getLogger(SpiderThread.class);
    private CrawlerWorkqueue queue;
    private volatile boolean busy;
    private DataWriter outputCollector;
    private String proxy;
    private int port;
    private DataExtractor extractor;
    private DataExtractor dirExtractor;
    private DataExtractor linkExtractor;
    private DataEnhancer[] dataEnhancers;
    private WebClient webClient;
    private WebClient backupWebClient;

    /**
     * creates a new SpiderThread object that will add its output to the
     * collector object passed in. If proxy is not null, it will use the proxy
     * server in the proxy parameter for all http requests.
     *
     * @param proxy     proxy host
     * @param port      proxy port
     * @param collector initialized DataWriter instance
     * @param enhancers optional array of data enhancers
     */
    public SpiderThread(String proxy, int port, CrawlerWorkqueue workQueue,
                        DataWriter collector, DataExtractor extractor,
                        DataEnhancer... enhancers) {
        this.proxy = proxy;
        this.port = port;

        queue = workQueue;
        this.extractor = extractor;
        this.dirExtractor = new DirectoryExtractor();
        this.linkExtractor = new HyperlinkExtractor();
        busy = true;
        outputCollector = collector;
        dataEnhancers = enhancers;
        webClient = initializeWebClient(false);
        backupWebClient = initializeWebClient(true);
    }

    public boolean isBusy() {
        return busy;
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
        int idleIterations = 0;
        while (busy) {
            try {
                String url = queue.dequeue();
                if (url != null) {
                    busy = true;
                    idleIterations = 0;
                    Page page = processItem(url);
                    if (page != null) {
                        Collection<DataRecord> drColl = extractor.extractData(
                                url, page);
                        if (drColl != null) {

                            for (DataRecord dr : drColl) {
                                if (dataEnhancers != null) {
                                    for (DataEnhancer enhancer : dataEnhancers) {
                                        if (enhancer != null) {
                                            if (dr != null) {
                                                dr = enhancer.enhanceData(dr);
                                            }
                                        }
                                    }
                                }
                                if (dr != null) {
                                    outputCollector.addItem(dr);
                                }
                            }
                        }
                    }
                } else {
                    if (idleIterations > MAX_IDLE_ITERATIONS) {
                        busy = false;
                    } else {
                        Thread.sleep(BACKOFF_INTERVAL);
                        idleIterations++;
                    }
                }
            } catch (InterruptedException e) {
                logger.error("Thread interrupt", e);
            }
        }
    }

    /**
     * attempts to process the item identified by thisPage in order to find
     * additional items to process. If thisPage refers to a local directory, the
     * DirectoryExtractor will be used to obtain a list of contents which will
     * be added to the workqueue.. If thisPage refers to a file (either local or
     * remote), a Page will be returned containing the content. If the page is
     * an HtmlPage, it will be searched for additional links and, if found, they
     * will be added to the workqueue.
     *
     * @param thisPage
     * @return
     */
    private Page processItem(String thisPage) {
        if (thisPage != null) {
            Collection<DataRecord> records = null;
            if (thisPage.startsWith(DirectoryExtractor.PROTOCOL)) {
                records = dirExtractor.extractData(thisPage, null);
                if (records != null) {
                    for (DataRecord r : records) {
                        if (DirectoryExtractor.DIR_RECORD_TYPE.equals(r
                                .getType())) {
                            for (Map.Entry<String, Object> field : r
                                    .getFields()) {
                                String link = (String) field.getValue();
                                if (link != null && !link.trim().equals(".")
                                        && !link.trim().equals("..")) {
                                    queue.enqueue(link, thisPage);
                                }
                            }
                        } else if (DirectoryExtractor.FILE_RECORD_TYPE.equals(r
                                .getType())) {
                            return processFile(thisPage, false);
                        }
                    }
                }
            } else {
                return processFile(thisPage, true);
            }
        }
        return null;
    }

    /**
     * processes a page by first attempting to read it with the webClient. If
     * the page returned is a HtmlPage, the links will be extracted and added to
     * the workqueue prior to returning the page data.
     *
     * @param thisPage
     * @param extractLinks - indicates whether links should be parsed from the page
     * @return
     */
    private Page processFile(String thisPage, boolean extractLinks) {
        try {
            // TODO: this can fail if running offline and the page attempts to
            // load remote JS
            Page page = fetchPage(thisPage);
            if (extractLinks) {
                Collection<DataRecord> records = linkExtractor.extractData(
                        thisPage, page);
                if (records != null) {
                    for (DataRecord r : records) {
                        for (Map.Entry<String, Object> field : r.getFields()) {
                            String link = (String) field.getValue();
                            if (link != null
                                    && !link.toLowerCase()
                                    .startsWith("mailto:")) {
                                queue.enqueue(link, thisPage);
                            }
                        }
                    }
                }
            }
            return page;
        } catch (FileNotFoundException e) {
            logger.info("File not found: " + thisPage);
        } catch (FailingHttpStatusCodeException e) {
            logger.error("HTTP Failure", e);
        } catch (MalformedURLException e) {
            logger.error("Bad URL", e);
        } catch (IOException e) {
            logger.error("IO Error", e);
        }
        return null;
    }

    private Page fetchPage(String url) throws IOException {
        Page p = null;
        try {
            p = webClient.getPage(url);
        } catch (RuntimeException rEx) {
            logger.warn(
                    "Could not load page with normal client. Trying backup");
            p = backupWebClient.getPage(url);
        }
        return p;
    }

    /**
     * initializes the WebClient object that will be used to fetch and parse web
     * pages
     *
     * @return new instance of WebClient that can be used to load and parse
     * pages
     */
    private WebClient initializeWebClient(boolean minimal) {

        WebClient client = null;
        if (proxy != null && !proxy.trim().isEmpty()) {
            client = new WebClient(BrowserVersion.CHROME, proxy, port);
        } else {
            client = new WebClient(BrowserVersion.CHROME);
        }
        if (minimal) {
            client.getOptions().setAppletEnabled(false);
            client.getOptions().setJavaScriptEnabled(false);
            client.getOptions().setCssEnabled(false);
        }
        return client;
    }
}
