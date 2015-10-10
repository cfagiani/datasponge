package org.cataractsoftware.datasponge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.cataractsoftware.datasponge.engine.JobCoordinator;
import org.cataractsoftware.datasponge.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import java.io.File;
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
    private static final long TERMINATE_CHECK_INTERVAL = 5000;
    private static Timer jobProgressTimer;
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
        File jobFile = null;
        boolean isSingleJob = false;
        boolean useEmbeddedBroker = false;
        boolean enableRest = false;
        if (args.length > 0) {
            List<String> argList = Arrays.asList(args);
            if (argList.contains("--help") || argList.contains("?")) {
                printHelp();
                System.exit(0);
            }

            if (argList.contains("--server")) {
                useEmbeddedBroker = true;
            }
            if (argList.contains("--restapi")) {
                enableRest = true;
            }

            int jobIdx = argList.indexOf("--job");
            if (jobIdx != -1) {
                jobFile = new File(argList.get(jobIdx + 1));
            }
            if (argList.contains("--singleJob")) {
                isSingleJob = true;
            }
            initialize(useEmbeddedBroker, enableRest, isSingleJob, jobFile, args);
        }else{
            System.out.println("Bad command line arguments\n");
            printHelp();
            System.exit(1);
        }


    }

    public static void initialize(boolean useEmbeddedBroker, boolean enableRest, boolean isSingleJob, File jobFile, String[] args) {
        SpringApplication app = new SpringApplication(DataSponge.class);
        List<String> enabledProfiles = new ArrayList<>();
        if (useEmbeddedBroker) {
            enabledProfiles.add("broker");
        }
        if (enableRest) {
            enabledProfiles.add("restapi");
        }
        if (enabledProfiles.size() > 0) {
            app.setAdditionalProfiles(enabledProfiles.toArray(new String[enabledProfiles.size()]));
        }
        ApplicationContext context = app.run(args);
        JobCoordinator coord = context.getBean(JobCoordinator.class);
        if (jobFile != null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                Job j = coord.submitJob(mapper.readValue(jobFile, Job.class));
                if (j != null) {
                    logger.info("Submitted job " + j.getGuid());
                }
            } catch (Exception e) {
                logger.error("Could not submit job from file", e);
            }
        }
        if (isSingleJob) {

            terminateUponCompletion(context, coord);
        }
    }

    private static void printHelp() {
        StringBuilder builder = new StringBuilder(String.format("Usage: java %s [--help] [--job <pathToJobFile>] [--server] [--singleJob] [--restapi]", DataSponge.class.getCanonicalName()));
        builder.append("\n--help: shows this message and exits");
        builder.append("\n--job <pathToJobFile>: automatically submits the job described by the file to the system and executes it");
        builder.append("\n--server: indicates that this node should run the message broker. Only 1 node in an ensemble should be run with this option");
        builder.append("\n--singleJob: indicates that the system should terminate after processing a single job");
        System.out.println(builder.toString());
    }

    private static void terminateUponCompletion(final ApplicationContext context, final JobCoordinator coordinator) {
        jobProgressTimer = new Timer();
        jobProgressTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (coordinator.areAllJobsDone()) {
                    ((AbstractApplicationContext) context).close();
                    jobProgressTimer.cancel();
                }
            }
        }, TERMINATE_CHECK_INTERVAL, TERMINATE_CHECK_INTERVAL);
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
    public JmsTemplate ouputJmsTemplate() {
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