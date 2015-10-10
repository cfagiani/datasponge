package org.cataractsoftware.datasponge.engine;

import org.cataractsoftware.datasponge.crawler.CrawlerWorkqueue;
import org.cataractsoftware.datasponge.crawler.SpiderThread;
import org.cataractsoftware.datasponge.enhancer.DataEnhancer;
import org.cataractsoftware.datasponge.extractor.DataExtractor;
import org.cataractsoftware.datasponge.model.Job;
import org.cataractsoftware.datasponge.util.ComponentFactory;
import org.cataractsoftware.datasponge.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * This bean is responsible for executing a crawl job on a single host. It is a prototype bean that is initialized in response
 * to an enrollment message.
 *
 * @author Christopher Fagiani
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobExecutor {

    private static final Logger logger = LoggerFactory
            .getLogger(JobExecutor.class);
    private static final int DEFAULT_THREADS = 5;
    private static final long DEFAULT_SLEEP = 5000;
    @Value("${proxyhost}")
    private String proxy;
    @Value("${proxyport}")
    private int port;
    private int maxThreads = DEFAULT_THREADS;
    private long sleepInterval = DEFAULT_SLEEP;
    private long crawlInterval;
    private Job jobDefinition;
    @Autowired
    private CrawlerWorkqueue workQueue;
    @Autowired
    private ComponentFactory componentFactory;

    private volatile boolean done;

    /**
     * creates N new SpiderThread objects (where N is the maxthreads property),
     * all of which point to the same Collector object. It will then spawn a new
     * thread for each SpiderThread object and start them. Once all threads are
     * started, this method will loop over the following steps until all threads
     * are idle:<br>
     * <ul>
     * <li>check if any thread is still busy</li>
     * <li>flush the collector</li>
     * <li>sleep for X milliseconds (configured via sleepinterval property)</li>
     * </ul>
     * <br>
     * once the threads are all idle, the collector will be closed and the
     * executor will terminate.
     */
    public void executeCrawl() {
        done = false;
        Thread crawlThread = new Thread(new Runnable() {
            @Override
            public void run() {
                DataExtractor extractor = componentFactory.getNewDataAdapter(jobDefinition.getGuid(), jobDefinition
                        .getDataExtractor());
                long startTime = System.currentTimeMillis();
                while (!done) {
                    long iterStartTime = System.currentTimeMillis();
                    DataWriter outputCollector = componentFactory.getNewDataAdapter(jobDefinition.getGuid(), jobDefinition
                            .getDataWriter());

                    DataEnhancer[] enhancers = componentFactory.getNewDataAdapterPipeline(jobDefinition.getGuid(), jobDefinition
                            .getDataEnhancers());

                    List<SpiderThread> threadList = spawnThreads(maxThreads,
                            outputCollector, extractor, enhancers);

                    while (areStillWorking(threadList)) {
                        try {
                            writeIncrementalOutput(outputCollector);
                            Thread.sleep(sleepInterval);
                        } catch (InterruptedException e) {
                            logger.error("thread interrupted", e);
                        } catch (IOException e) {
                            logger.error("Couldn't write incremental output", e);
                        }
                    }
                    outputCollector.finish();
                    logger.info("Crawl iteration took {} seconds",
                            ((System.currentTimeMillis() - iterStartTime) / 1000));
                    if (Job.Mode.ONCE == jobDefinition.getMode()) {
                        done = true;
                    } else {
                        try {
                            Thread.sleep(crawlInterval);
                        } catch (InterruptedException iEx) {
                            logger.error(
                                    "Thread interrupt while sleeping between crawl iterations",
                                    iEx);
                        }
                    }
                }

                long totalTime = System.currentTimeMillis() - startTime;
                logger.info("Crawl ran for {} seconds", (totalTime / 1000));
            }
        });
        crawlThread.start();
    }

    public boolean isDone() {
        return done;
    }

    /**
     * flushes the outputCollector and prints the current size of the queue to
     * standard out
     *
     * @param outputCollector initialized DataWriter instance that will collect data as it
     *                        is discovered
     * @throws IOException
     */
    private void writeIncrementalOutput(DataWriter outputCollector)
            throws IOException {
        outputCollector.flushBatch();
    }

    /**
     * checks to see if any of the SpiderThreads in the threadList are still
     * working. If so, it returns true, if not, return false.
     *
     * @param threadList list of SpiderThreads
     * @return true if one or more threads are still busy
     */
    private boolean areStillWorking(List<SpiderThread> threadList) {
        for (SpiderThread st : threadList) {
            if (st.isBusy()) {
                return true;
            }
        }
        return false;
    }

    /**
     * create threadCount new SpiderThreads and start them.
     *
     * @param threadCount     number of threads to spawn
     * @param outputCollector initialized DataWriter instance that will collect data as it
     *                        is discovered
     * @param extractor       initialized DataExtractor instance that will extract data from
     *                        each page
     * @param enhancers       optional list of data enhancers
     * @return - list of running threads
     */
    private List<SpiderThread> spawnThreads(int threadCount,
                                            DataWriter outputCollector, DataExtractor extractor,
                                            DataEnhancer... enhancers) {
        List<SpiderThread> threadList = new ArrayList<SpiderThread>();
        for (int i = 0; i < threadCount; i++) {
            SpiderThread st = new SpiderThread(proxy, port, workQueue,
                    outputCollector, extractor, enhancers);
            threadList.add(st);
            Thread t = new Thread(st);
            t.start();
        }
        return threadList;
    }

    /**
     * handles node failures. Returns true if the nodeId passed in corresponds to this node
     * @param nodeId
     * @return
     */
    public boolean handleNodeFailure(int nodeId){
        return workQueue.handleNodeFailure(nodeId);
    }

    /**
     * initializes the crawler program by loading the properties, creating the
     * common work queue and seeding it with the list of URLs at which to start.
     */
    public void init(Job jobDefinition, int nodeId, int modSize, boolean doSeed) {

        if (jobDefinition.getMaxThreads() > 0) {
            maxThreads = jobDefinition.getMaxThreads();
        }
        this.jobDefinition = jobDefinition;
        this.crawlInterval = jobDefinition.getContinuousCrawlInterval() != null ? jobDefinition.getContinuousCrawlInterval() : 1000L;

        workQueue.initialize(jobDefinition.getGuid(),
                jobDefinition.getIgnorePatterns(),
                jobDefinition.getIncludePatterns(), nodeId, modSize);
        if (doSeed) {
            seedQueue(jobDefinition.getStartUrls());
        }
    }

    private void seedQueue(Set<String> list) {
        if (list != null) {
            for (String item : list) {
                workQueue.enqueue(item, null);
            }
        }
    }

    public void destroy() {

    }


}
