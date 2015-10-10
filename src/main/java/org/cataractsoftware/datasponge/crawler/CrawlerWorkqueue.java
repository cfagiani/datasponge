package org.cataractsoftware.datasponge.crawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
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
 * If the url passes the regex checks, it will be submitted to a JMS topic. This class also serves as a message listener for that topic BUT
 * it will only process the messages that have URLs that have this host's nodeId as the 'target' JMS property.
 * <p/>
 * When receiving messages that correspond to this host, the url is placed in an internal datastore that behaves like a FIFO queue.
 *
 * @author Christopher Fagiani
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class
        CrawlerWorkqueue {
    private static final String SELECTOR_PROP = "target";

    private static final Logger logger = LoggerFactory
            .getLogger(CrawlerWorkqueue.class);
    private Set<String> processedUrls;
    private Set<String> ignoreList;
    private Set<String> includeList;
    private Queue<String> queue;
    @Resource(name = "workQueueTemplate")
    private JmsTemplate workQueueTemplate;
    private String jobId;
    private String selectorVal;
    private int modSize;
    private int nodeId;


    private CrawlerWorkqueue() {
        processedUrls = new HashSet<String>();
        queue = new ConcurrentLinkedQueue<String>();

    }

    /**
     * set up all the member variables used for admitting/rejecting urls.
     *
     * @param jobId
     * @param excludeList
     * @param includes
     * @param nodeId
     * @param modSize
     */
    public void initialize(String jobId, Set<String> excludeList,
                           Set<String> includes, int nodeId, int modSize) {
        this.jobId = jobId;
        if (excludeList != null) {
            ignoreList = excludeList;
        } else {
            ignoreList = new HashSet<String>();
        }
        if (includes != null) {
            includeList = includes;
        } else {
            includeList = new HashSet<String>();
        }
        this.nodeId = nodeId;
        this.modSize = modSize;
        updateSelector();
    }

    /**
     * sets the value to use for jms selectors
     */
    private void updateSelector(){
        this.selectorVal = this.jobId + "-"+this.nodeId;
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
            if (pageUrl != null) {
                url = makeAbsolute(url, pageUrl);
            }

            if (!processedUrls.contains(url) && isInList(url, includeList)
                    && !isInList(url, ignoreList)) {
                final String targetUrl = url;
                MessageCreator messageCreator = new MessageCreator() {
                    @Override
                    public Message createMessage(Session session)
                            throws JMSException {
                        try {

                            Message m = session.createTextMessage(targetUrl);

                            m.setStringProperty(SELECTOR_PROP, jobId + "-"
                                    + (targetUrl.hashCode() % modSize));
                            return m;
                        } catch (Exception e) {
                            logger.error(
                                    "Could not publish work item as json to jms",
                                    e);
                            throw new JMSException(
                                    "Could not publish work item: "
                                            + e.getMessage());
                        }
                    }
                };
                workQueueTemplate.send(messageCreator);
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
            if (pageUrl.startsWith("file:")) {
                if (pageUrl.endsWith("/")) {
                    url = pageUrl + url;
                } else {
                    url = pageUrl + "/" + url;
                }
            } else {
                // find the index of the first "/" that isn't in the protocol
                // section
                int idxOfEndOfProtocol = pageUrl.indexOf("//") + 2;
                int idxOfFirstSlash = pageUrl.indexOf("/", idxOfEndOfProtocol);
                if (url.startsWith("/")) {
                    // if it starts with "/" then we need the root
                    if (idxOfFirstSlash > 0) {
                        // if the pageURL contains a / then we need to get just
                        // up
                        // to that
                        url = pageUrl.substring(0, idxOfFirstSlash) + linkUrl;
                    } else {
                        // if the pageURL doesn't have a trailing slash, just
                        // concatenate
                        url = pageUrl + linkUrl;
                    }
                } else {
                    // if the linkUrl doesn't start with "/" then it's relative
                    // to
                    // the directory of the pageUrl
                    int idxOfLastSlash = pageUrl.lastIndexOf("/");
                    if (idxOfLastSlash > 0
                            && idxOfLastSlash > idxOfEndOfProtocol) {
                        url = pageUrl.substring(0, idxOfLastSlash) + "/"
                                + linkUrl;
                    } else {
                        url = pageUrl + "/" + linkUrl;
                    }
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
     * @param list set of regular expressions against which the url will be
     *             checked
     * @return true if url matches at least 1 expression
     */
    private boolean isInList(String url, Set<String> list) {
        boolean matches = false;
        for (String s : list) {
            matches = url.matches(s);
            if (matches) {
                break;
            }
        }
        return matches;
    }

    /**
     * clears internal datastructures
     */
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
     * respond to node failures by adjusting the selector used to determine which messages should be handled by this node.
     * @param failedNodeId
     * @return - true if the failed node is THIS node, false if not
     */
    public synchronized boolean handleNodeFailure(int failedNodeId) {
        if(this.nodeId == failedNodeId){
            return true;
        }
        if(this.nodeId> failedNodeId){
            this.nodeId--;
            updateSelector();
        }
        this.modSize--;
        return false;
    }


    /**
     * called by the message listener container in response to receipt of a JMS message. This method will compare the value of the
     * target property to this node's hostId and, if it matches, will add it to the internal queue if the url hasn't already been processed.
     *
     * @param msg
     * @param selector
     */
    @JmsListener(id = "dataspongeworkqueue", destination = "datasponge.workqueue.topic", containerFactory = "topicContainerFactory")
    public void handleWorkMessage(@Payload String msg, @Header(SELECTOR_PROP) String selector) {
        if (this.selectorVal.equals(selector)) {
            if (!processedUrls.contains(msg)) {
                processedUrls.add(msg);
                queue.add(msg);
            }
        }
    }

}
