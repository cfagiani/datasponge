package org.cataractsoftware.datasponge.crawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is thread-safe data structure for use in a web crawler/spider. It
 * maintains an internal list of URLs to visit as well as lists of urls already
 * visited. In addition to these lists, it maintains 2 lists of regular
 * expressions that are used to determine whether or not to admit a url to the
 * pending list.
 * <p/>
 * Calling enqueue does not guarantee that a url will be added to the internal
 * store. it will only be added if it matches a regex in the includeList AND it
 * does NOT match a regex in the ignoreList (configured via the constructor).
 * <p/>
 * The internal data store behaves like a FIFO queue.
 *
 * @author Christopher Fagiani
 */
public class CrawlerWorkqueue {

    private static final Logger logger = LoggerFactory.getLogger(CrawlerWorkqueue.class);
    private HashSet<String> processedUrls;
    private HashSet<String> ignoreList;
    private HashSet<String> includeList;
    private static CrawlerWorkqueue workqueue;
    private Queue<String> queue;

    /**
     * creates a new instance
     *
     * @param excludeList -
     *                    list of regular expressions that should be used to EXCLUDE
     *                    urls from the work queue
     * @param includes    -
     *                    list of regular expressions that are used to include urls
     */
    private CrawlerWorkqueue(HashSet<String> excludeList,
                             HashSet<String> includes) {
        processedUrls = new HashSet<String>();
        ignoreList = excludeList;
        queue = new ConcurrentLinkedQueue<String>();
        includeList = includes;

    }

    /**
     * creates a singleton instance
     *
     * @param excludeList list of regular expressions that should be used to EXCLUDE
     *                    urls from the work queue
     * @param includes    list of regular expressions that are used to include urls
     * @return new instance of this class
     */
    public static CrawlerWorkqueue createInstance(HashSet<String> excludeList,
                                                  HashSet<String> includes) {
        workqueue = new CrawlerWorkqueue(excludeList, includes);
        return workqueue;
    }

    /**
     * returns a singleton instance. Should only be used after createInstance is
     * called BUT if you call this before calling createInstance, it will return
     * a newly created singleton instance that uses null for both includeList
     * and excludeList (basically worthless.)
     *
     * @return singleton instance of this class, initializing if needed
     */
    public static CrawlerWorkqueue getInstance() {
        if (workqueue == null) {
            workqueue = new CrawlerWorkqueue(new HashSet<String>(),
                    new HashSet<String>());
        }
        return workqueue;
    }

    /**
     * This method will first strip off any anchors from the url (portions of
     * the url after #). It will then convert relative urls to absolute urls
     * (using values from the pageUrl). Then, if the subsequent url matches at
     * least 1 regex in the includeList and does NOT match any regex in the
     * excludelist, it is added to the internal workqueue.
     *
     * @param url     url to be added
     * @param pageUrl url of page on which the url was found
     */
    public synchronized void enqueue(String url, String pageUrl) {
        if (url != null && url.trim().length() > 0 && !url.trim().equals("/")) {
            url = url.trim();
            if (url.contains("#")) {
                url = url.substring(0, url.indexOf("#"));
            }
            // if it's a relative url, make it absolute
            url = makeAbsolute(url, pageUrl);

            if (!processedUrls.contains(url) && isInList(url, includeList)
                    && !isInList(url, ignoreList)) {
                queue.add(url);
                processedUrls.add(url);
            }
        }
    }

    /**
     * utility method for converting a relative url to an absolute url.
     *
     * @param linkUrl possibly relative url
     * @param pageUrl url of page on which linkUrl was found
     * @return absolute url
     */
    private String makeAbsolute(String linkUrl, String pageUrl) {
        String url = linkUrl.trim();
        if (!url.startsWith("http")) {
            // find the index of the first "/" that isn't in the protocol
            // section
            int idxOfEndOfProtocol = pageUrl.indexOf("//") + 2;
            int idxOfFirstSlash = pageUrl.indexOf("/",
                    idxOfEndOfProtocol);
            if (url.startsWith("/")) {
                // if it starts with "/" then we need the root
                if (idxOfFirstSlash > 0) {
                    // if the pageURL contains a / then we need to get just up
                    // to that
                    url = pageUrl.substring(0, idxOfFirstSlash) + linkUrl;
                } else {
                    // if the pageURL doesn't have a trailing slash, just
                    // concatenate
                    url = pageUrl + linkUrl;
                }
            } else {
                // if the linkUrl doesn't start with "/" then it's relative to
                // the directory of the pageUrl
                int idxOfLastSlash = pageUrl.lastIndexOf("/");
                if (idxOfLastSlash > 0 && idxOfLastSlash > idxOfEndOfProtocol) {
                    url = pageUrl.substring(0, idxOfLastSlash) + "/" + linkUrl;
                } else {
                    url = pageUrl + "/" + linkUrl;
                }
            }
        }
        return url;
    }

    /**
     * compares url to list of regular expressions. It will return true if at
     * least 1 regex matches the url
     *
     * @param url  url to check
     * @param list set of regular expressions against which the url will be checked
     * @return true if url matches at least 1 expression
     */
    private boolean isInList(String url, HashSet<String> list) {
        boolean matches = false;
        for (String s : list) {
            matches = url.matches(s);
            if (matches) {
                break;
            }
        }
        return matches;
    }

    public void reset() {
        if (processedUrls != null) {
            processedUrls.clear();

        }
        if (queue != null) {
            queue.clear();
        }
    }

    /**
     * pops the item off the head of the queue. If the queue is empty, this will
     * return null. NOTE: this is different than the normal behavior of the Java
     * Queue class (which will throw an exception if you try to call pop on an
     * empty queue).
     *
     * @return next item from the queue
     */
    public synchronized String dequeue() {
        String item = null;
        try {
            item = queue.remove();
            logger.info("VISITING: {}", item);
        } catch (NoSuchElementException e) {
            item = null;
        }
        return item;
    }

    /**
     * returns the size of the work queue
     *
     * @return size of queue
     */
    public int getSize() {
        if (queue != null) {
            return queue.size();
        } else {
            return 0;
        }
    }

}

