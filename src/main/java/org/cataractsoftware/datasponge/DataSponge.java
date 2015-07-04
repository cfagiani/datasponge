package org.cataractsoftware.datasponge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.cataractsoftware.datasponge.crawler.CrawlerWorkqueue;
import org.cataractsoftware.datasponge.engine.JobCoordinator;
import org.cataractsoftware.datasponge.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Utility to spider a web site and extract data from pages. The utility is
 * configured through a property file (path to the file is passed in as a
 * command line argument).
 * <p/>
 * The sponge will start at a set of given URLs, crawl all links and parse the
 * results. The scope of the crawl can be narrowed using inclusion/exclusion
 * regular expressions (set in the properties file). As it crawls, data will be
 * added to an internal collection. This collection is periodically flushed to
 * facilitate batch submission.
 * <p/>
 * The utility is meant to be extended by users, supplying their own
 * DataExtractor and DataWriter instances as needed.
 *
 * @author Christopher Fagiani
 */
@SpringBootApplication
@EnableJms
public class DataSponge {

    private static final Logger logger = LoggerFactory
            .getLogger(DataSponge.class);
    private Properties props;
    @Value("${proxyhost}")
    private String proxy;
    @Value("${proxyport}")
    private int port;
    @Value("${jms.broker.url}")
    private String jmsBrokerUrl;

    /**
     * constructs a new object using the properties loaded into p
     *
     * @param p initialized properties object
     */
    public DataSponge(Properties p) {
        props = p;
    }

    /**
     * default constructor. If this is uses, all config options needs to be set
     * prior to calling the executeCrawl method.
     */
    public DataSponge() {
        props = new Properties();
    }

    /**
     * main method for this program. It checks the command line arguments and,
     * if valid, loads the properties and instantiates an object of this class.
     * Once the object is instantiated it will initialize it and then start the
     * crawl process.
     *
     * @param args command line arguments containing the path to the property
     *             file
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(DataSponge.class);
        File jobFile = null;
        boolean isServer = false;
        if (args.length > 0) {
            List<String> argList = Arrays.asList(args);
            if (argList.contains("--server")) {
                isServer = true;
                app.setAdditionalProfiles("broker");
            }
            int jobIdx = argList.indexOf("--job");
            if (jobIdx != -1) {
                jobFile = new File(argList.get(jobIdx + 1));
            }
        }
        System.out.println("Starting app...");
        ApplicationContext context = app.run(args);
        if (jobFile != null && isServer) {
            JobCoordinator coord = context.getBean(JobCoordinator.class);
            ObjectMapper mapper = new ObjectMapper();
            try {
                coord.submitJob(mapper.readValue(jobFile, Job.class));
            } catch (Exception e) {
                logger.error("Could not submit job from file", e);
            }
        }
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
     * @param queue work queue of pending pages (links discovered but not yet
     *              visited)
     * @param list  list of urls to add to queue
     */
    private static void seedQueue(CrawlerWorkqueue queue, HashSet<String> list) {
        queue.reset();
        if (list != null) {
            for (String item : list) {
                queue.enqueue(item, null);
            }
        }
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        return new ActiveMQConnectionFactory(getJmsBrokerUrl());
    }

    @Bean
    public JmsTemplate jobTopicTemplate() {
        JmsTemplate template = new JmsTemplate(connectionFactory());
        template.setDefaultDestinationName("datasponge.job.topic");
        template.setPubSubDomain(true);
        return template;

    }

    @Bean
    public JmsTemplate ouputJmsTemplate(){
        JmsTemplate template = new JmsTemplate(connectionFactory());
        template.setDefaultDestinationName("datasponge.output.topic");
        template.setPubSubDomain(true);
        return template;
    }

    @Bean
    public JmsTemplate managementTopicTemplate() {
        JmsTemplate template = new JmsTemplate(connectionFactory());
        template.setDefaultDestinationName("datasponge.management.topic");
        template.setPubSubDomain(true);
        return template;
    }

    @Bean
    public JmsTemplate workQueueTemplate() {
        JmsTemplate template = new JmsTemplate(connectionFactory());
        template.setDefaultDestinationName("datasponge.workqueue.topic");
        template.setPubSubDomain(true);
        return template;
    }

    @Bean
        // this is here to bypass errors with auto-configuration since we have
        // multiple JMS templates
    JmsMessagingTemplate jmsMessagingTemplate() {
        return new JmsMessagingTemplate(jobTopicTemplate());
    }

    @Bean
    public DefaultJmsListenerContainerFactory topicContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory());
        // for this connection factory, we only want 1 instance; increasing this
        // will result in multiple copies of the same message locally
        factory.setConcurrency("1-1");
        factory.setPubSubDomain(true);
        return factory;
    }




    /**
     * sets additional properties (use if you need to set properties needed by
     * custom extractors/writers)
     *
     * @param name
     * @param value
     */
    public void setProperty(String name, String value) {
        props.setProperty(name, value);
    }



    private String getJmsBrokerUrl() {
        return jmsBrokerUrl;
    }
}