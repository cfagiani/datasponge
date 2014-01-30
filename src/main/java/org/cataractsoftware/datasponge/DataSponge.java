package org.cataractsoftware.datasponge;

import org.cataractsoftware.datasponge.crawler.CrawlerWorkqueue;
import org.cataractsoftware.datasponge.crawler.SpiderThread;
import org.cataractsoftware.datasponge.extractor.DataExtractor;
import org.cataractsoftware.datasponge.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Utility to spider a web site and extract data from pages. The utility is configured through a property file (path to the file is passed in as a command line argument).
 * <p/>
 * The sponge will start at a set of given URLs, crawl all links and parse the results. The scope of the crawl can be narrowed using inclusion/exclusion regular expressions (set in the properties file).
 * As it crawls, data will be added to an internal collection. This collection is periodically flushed to facilitate batch submission.
 * <p/>
 * The utility is meant to be extended by users, supplying their own DataExtractor and DataWriter instances as needed.
 *
 * @author Christopher Fagiani
 */
public class DataSponge {

    private static final Logger logger = LoggerFactory.getLogger(DataSponge.class);
    private static final String PROXYHOST = "proxyhost";
    private static final String PROXYPORT = "proxyport";
    private static final String STARTURLS = "starturls";
    private static final String IGNOREPATTERNS = "ignorepatterns";
    private static final String MAXTHREADS = "maxthreads";
    private static final String INTERVAL = "sleepinterval";
    private static final String INCLUDES = "includepatterns";
    private static final String DATAEXTRACTOR = "dataextractor";
    private static final String DATAWRITER = "datawriter";
    private Properties props;
    private CrawlerWorkqueue queue;
    private long sleepInterval;
    private String proxy;
    private int port;


    /**
     * constructs a new object using the properties loaded into p
     *
     * @param p initialized properties object
     */
    public DataSponge(Properties p) {
        props = p;
    }

    /**
     * default constructor. If this is uses, all config options needs to be set prior to calling the executeCrawl method.
     */
    public DataSponge() {
        props = new Properties();
    }

    public void setMaxThreads(int threads) {
        props.setProperty(MAXTHREADS, String.valueOf(threads));
    }

    public void setProxy(String host, int port) {
        props.setProperty(PROXYHOST, host);
        props.setProperty(PROXYPORT, String.valueOf(port));
    }

    public void setStartUrls(String urlList) {
        props.setProperty(STARTURLS, urlList);
    }

    public void setSleepInterval(int intervalMillis) {
        props.setProperty(INTERVAL, String.valueOf(intervalMillis));
    }

    public void setIncludePatterns(String patterns) {
        props.setProperty(INCLUDES, patterns);
    }

    public void setExcludePatterns(String patterns) {
        props.setProperty(IGNOREPATTERNS, patterns);
    }

    public void setWriterClass(String className) {
        props.setProperty(DATAWRITER, className);
    }

    public void setExtractorClass(String className) {
        props.setProperty(DATAEXTRACTOR, className);
    }

    /**
     * sets additional properties (use if you need to set properties needed by custom extractors/writers)
     *
     * @param name
     * @param value
     */
    public void setProperty(String name, String value) {
        props.setProperty(name, value);
    }


    /**
     * main method for this program. It checks the command line arguments and,
     * if valid, loads the properties and instantiates an object of this class.
     * Once the object is instantiated it will initialize it and then start the
     * crawl process.
     *
     * @param args command line arguments containing the path to the property file
     */
    public static void main(String[] args) {
        Properties props = null;

        if (args.length != 1) {
            System.out.println("Incorrect command line arguments.\n");
            printUsage();
            System.exit(0);
        }

        try {
            props = loadProps(args[0]);
        } catch (IOException e) {
            logger.error("Cannot load config file: ", e);
            System.exit(1);
        }
        DataSponge sponge = new DataSponge(props);
        sponge.executeCrawl();
    }

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
     * program will terminate.
     */
    public void executeCrawl() {
        init();
        long startTime = System.currentTimeMillis();
        int threadCount = 5;
        if (props.getProperty(MAXTHREADS) != null) {
            threadCount = Integer.parseInt(props.getProperty(MAXTHREADS));
        }

        DataWriter outputCollector = getNewDataAdapter(props.getProperty(DATAWRITER), props);
        DataExtractor extractor = getNewDataAdapter(props.getProperty(DATAEXTRACTOR), props);

        List<SpiderThread> threadList = spawnThreads(threadCount,
                outputCollector, extractor);

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

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Crawl took {} seconds", (totalTime / 1000));
    }

    /**
     * flushes the outputCollector and prints the current size of the queue to
     * standard out
     *
     * @param outputCollector initialized DataWriter instance that will collect data as it is discovered
     * @throws IOException
     */
    private void writeIncrementalOutput(DataWriter outputCollector)
            throws IOException {
        outputCollector.flushBatch();
        logger.info("Queue Size: {}", queue.getSize());
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
     * @param outputCollector initialized DataWriter instance that will collect data as it is discovered
     * @param extractor       initialized DataExtractor instance that will extract data from each page
     * @return - list of running threads
     */
    private List<SpiderThread> spawnThreads(int threadCount,
                                            DataWriter outputCollector, DataExtractor extractor) {
        List<SpiderThread> threadList = new ArrayList<SpiderThread>();
        for (int i = 0; i < threadCount; i++) {
            SpiderThread st = new SpiderThread(proxy, port, outputCollector,
                    extractor);
            threadList.add(st);
            Thread t = new Thread(st);
            t.start();
        }
        return threadList;
    }

    /**
     * initializes the crawler program by loading the properties, creating the
     * common work queue and seeding it with the list of URLs at which to start.
     */
    private void init() {
        try {
            proxy = props.getProperty(PROXYHOST);
            port = 0;
            if (props.getProperty(PROXYPORT) != null) {
                port = Integer.parseInt(props.getProperty(PROXYPORT));
            }
            sleepInterval = 10000;
            if (props.getProperty(INTERVAL) != null) {
                sleepInterval = Integer.parseInt(props.getProperty(INTERVAL));
            }

            queue = CrawlerWorkqueue.createInstance(parseList(props
                    .getProperty(IGNOREPATTERNS)), parseList(props
                    .getProperty(INCLUDES)));
            seedQueue(queue, parseList(props.getProperty(STARTURLS)));
        } catch (Exception e) {
            logger.error("Could not initialize the system.", e);
            System.exit(1);
        }

    }

    /**
     * prints command line argument usage
     */
    public static void printUsage() {
        System.out.println("Usage:\njava org.cataractsoftware.datasponge DataSponge <configFile>");
    }

    /**
     * loads the property file designated by file into a Properties object and
     * returns it to caller.
     *
     * @param file fully qualified file name for property file
     * @return Properties object loaded with values from file
     * @throws IOException
     */
    public static Properties loadProps(String file) throws IOException {
        Properties props = new Properties();
        InputStream propertyStream = new FileInputStream(file);
        props.load(propertyStream);
        propertyStream.close();
        return props;
    }

    /**
     * parses a string that contains a comma-delimited list of values and
     * returns them as a HashSet
     *
     * @param list string that contains a comma-delimited list of values
     * @return set of strings parsed from the input
     */
    private static HashSet<String> parseList(String list) {
        HashSet<String> set = new HashSet<String>();
        if (list != null) {
            StringTokenizer strTok = new StringTokenizer(list, ",");
            while (strTok.hasMoreTokens()) {
                set.add(strTok.nextToken());
            }
        }
        return set;
    }

    /**
     * iterates over each entry in list and adds them to the work queue
     *
     * @param queue work queue of pending pages (links discovered but not yet visited)
     * @param list  list of urls to add to queue
     */
    private static void seedQueue(CrawlerWorkqueue queue, HashSet<String> list) {
        if (list != null) {
            for (String item : list) {
                queue.enqueue(item, null);
            }
        }
    }

    /**
     * helper method to reflectively instantiate and initialize the pluggable DataAdapter components (DataExtractor and DataWriter instances).
     *
     * @param className class to instantiate
     * @param props     property object containing all the properties from the property file used to run the program
     * @param <T>       subtype of DataAdapter
     * @return initialized instance of T
     */
    @SuppressWarnings("unchecked")
    private <T extends DataAdapter> T getNewDataAdapter(String className, Properties props) {
        try {
            Class writerClass = Class.forName(className);
            T adapter = (T) writerClass.newInstance();
            adapter.init(props);
            return adapter;
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException("Could not instantiate " + className + " as DataAdapter. Ensure " + DATAWRITER + " property is a fully qualified class name and it is on the classpath", ex);
        }
    }


}