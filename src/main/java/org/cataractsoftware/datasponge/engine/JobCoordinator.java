package org.cataractsoftware.datasponge.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.model.Job;
import org.cataractsoftware.datasponge.model.JobEnrollment;
import org.cataractsoftware.datasponge.model.ManagementMessage;
import org.cataractsoftware.datasponge.model.ManagementMessage.Type;
import org.cataractsoftware.datasponge.util.ComponentFactory;
import org.cataractsoftware.datasponge.writer.DataWriter;
import org.cataractsoftware.datasponge.writer.JmsDataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.*;

/**
 * This component is responsible for coordination of any jobs. If running in a multi-node setup, there could be multiple coordinators
 * in a single ensemble. In that case, the coordinator on the node to which the job was submitted is responsible for that job.
 */
@Component
public class JobCoordinator{
    private static final Logger logger = LoggerFactory
            .getLogger(JobCoordinator.class);

    private static final long OUTPUT_FLUSH_INTERVAL = 3000;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int WAIT_TIME_SECS = 10;
    private static final String UNKNOWN_JOB_MSG = "UNKNOWN_JOB";
    private static final String HOST_KEY = "host";

    private static final String NODE_KEY = "nodeId";
    private static final String SIZE_KEY = "modSize";
    private static final String HOST_ID = UUID.randomUUID().toString();

    @Resource(name = "jobTopicTemplate")
    private JmsTemplate jobTopicTemplate;

    @Resource(name = "managementTopicTemplate")
    private JmsTemplate managementTopicTemplate;

    @Autowired
    private ComponentFactory componentFactory;

    private Map<String, Job> jobMap = new HashMap<String, Job>();
    private Map<String,DataWriter> dataWriterMap = new HashMap<String,DataWriter>();
    private Map<String, JobExecutor> jobExecutorMap = new HashMap<String, JobExecutor>();
    private Map<String, List<JobEnrollment>> enrollmentMap = new HashMap<String, List<JobEnrollment>>();
    private Timer jobProgressTimer;

    public JobCoordinator(){
        jobProgressTimer = new Timer();
        jobProgressTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                checkJobStatus();
            }
        }, OUTPUT_FLUSH_INTERVAL, OUTPUT_FLUSH_INTERVAL);
    }

    /**
     * returns the status of a job indentified by the id passed in as a String.
     * @param guid
     * @return
     */
    public String getJobStatus(String guid) {
        Job j = jobMap.get(guid);
        if (j != null) {
            return j.getStatus().toString();
        } else {
            return UNKNOWN_JOB_MSG;
        }
    }

    /**
     * submits a job to the ensemble via a JMS message to the control topic and assigns itself as both the coordinator AND a participant in the job.
     * It will then wait for a configurable amount of time before sending the ASSIGNMENT messages (thereby assigning each participant
     * in the job an ID that is used to partition the space of urls to be crawled.)
     *
     * @param j
     * @return
     */
    public Job submitJob(final Job j) {
        if (j != null) {
            j.setGuid(UUID.randomUUID().toString());
            j.setCoordinatorId(HOST_ID);
            if(j.getCoordinatorDataWriter() != null){
                dataWriterMap.put(j.getGuid(),(DataWriter)componentFactory.getNewDataAdapter(j.getGuid(), j.getCoordinatorDataWriter()));
            }
            MessageCreator messageCreator = new MessageCreator() {
                @Override
                public Message createMessage(Session session)
                        throws JMSException {
                    try {
                        Message m = session.createTextMessage(mapper
                                .writeValueAsString(j));
                        jobMap.put(j.getGuid(), j);
                        return m;
                    } catch (Exception e) {
                        logger.error("Could not publish job as json to jms", e);
                        throw new JMSException("Could not publish job: "
                                + e.getMessage());
                    }
                }
            };
            jobTopicTemplate.send(messageCreator);
            sendEnrollment(j.getGuid());
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    List<JobEnrollment> enrollments = enrollmentMap.get(j
                            .getGuid());
                    if (enrollments != null) {
                        for (int i = 0; i < enrollments.size(); i++) {
                            sendAssignment(j.getGuid(), i, enrollments.size());
                        }
                    }
                }
            }, WAIT_TIME_SECS * 1000);

        }
        return j;
    }

    /**
     * flushes output for any coordinator-writers and checks for completed jobs. If a job is completed, its status is
     * updated and the executor resources are recovered.
     */
    private void checkJobStatus(){
        flushOutput();
        List<String> completedJobs = new ArrayList<String>();
        for(Map.Entry<String,JobExecutor> executorEntry: jobExecutorMap.entrySet()){
            if(executorEntry.getValue().isDone()){
                String jobId = executorEntry.getKey();
                completedJobs.add(jobId);
                DataWriter writer = dataWriterMap.get(jobId);
                if(writer != null){
                    writer.finish();
                    dataWriterMap.remove(jobId);
                }
            }
        }
        if(completedJobs.size()>0){
            for(String jobId: completedJobs){
                jobExecutorMap.remove(jobId);
                logger.info("Removed executor for job "+jobId);
                Job job = jobMap.get(jobId);
                if(job != null){
                    job.setStatus(Job.Status.COMPLETE);
                }
            //TODO: need to unregister the JMS listeners
            }
        }
    }

    /**
     * iterates over all dataWriters and flushes their output
     */
    private void flushOutput(){
        for(DataWriter writer: dataWriterMap.values()){
            writer.flushBatch();
        }
    }

    /**
     * sends the ASSIGNMENT message on the control topic to assign an ID to each participant.
     * @param jobId
     * @param nodeId
     * @param modSize
     */
    private void sendAssignment(String jobId, int nodeId, int modSize) {
        ManagementMessage assignmentMessage = new ManagementMessage();
        assignmentMessage.setJobId(jobId);
        assignmentMessage.setType(Type.ASSIGNMENT);
        Map<String, String> data = new HashMap<String, String>();
        data.put(NODE_KEY, nodeId + "");
        data.put(SIZE_KEY, modSize + "");
        assignmentMessage.setData(data);
        managementTopicTemplate.send(buildMessageCreator(assignmentMessage));
    }

    /**
     * respond to job messages by enrolling.
     * @param message
     */
    @JmsListener(destination = "datasponge.job.topic", containerFactory = "topicContainerFactory")
    public void handleJobMessage(String message) {
        try {
            Job job = mapper.readValue(message, Job.class);
            if (!jobMap.containsKey(job.getGuid())) {
                // if we don't already know about this job
                jobMap.put(job.getGuid(), job);
                //TODO: perform check to ensure this node can handle the job (i.e. no ClassNotFoundException when instantiating DataAdapters)
                sendEnrollment(job.getGuid());
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not process job message", e);
        }
    }

    /**
     * dispatches messages received on the management topic
     * @param message
     */
    @JmsListener(destination = "datasponge.management.topic", containerFactory = "topicContainerFactory")
    public void handleManagementMessage(String message) {
        try {
            ManagementMessage msg = mapper.readValue(message,
                    ManagementMessage.class);
            switch (msg.getType()) {
                case ENROLLMENT:
                    handleEnrollmentMessage(msg);
                    break;
                case ASSIGNMENT:
                    initializeEngineForJob(msg.getJobId(),
                            Integer.parseInt(msg.getData().get(NODE_KEY)),
                            Integer.parseInt(msg.getData().get(SIZE_KEY)),
                            isJobCoordinator(msg.getJobId()));

                    break;
                case HEARTBEAT:
                    //TODO: track heartbeats and re-assign node keys based on size of ensemble
                    break;
                default:
                    break;

            }
        } catch (Exception e) {
            throw new RuntimeException("Could not process job message", e);
        }
    }

    /**
     * records an enrollment
     * @param msg
     */
    protected void handleEnrollmentMessage(ManagementMessage msg) {
        String jobId = msg.getJobId();

        // technically shouldn't need this since our container
        // factory is not parallel but it's here in case that is
        // changed
        synchronized (enrollmentMap) {
            List<JobEnrollment> enrollments = enrollmentMap.get(jobId);
            if (enrollments == null) {
                enrollments = new ArrayList<JobEnrollment>();
                enrollmentMap.put(jobId, enrollments);
            }
            JobEnrollment enrollment = new JobEnrollment(jobId, msg.getData()
                    .get(HOST_KEY));
            enrollments.add(enrollment);
        }
    }

    /**
     * received DataRecords that were sent by the participants in the crawl. This is used in conjunction with coordniatorDataWriter configuration to aggregate all output. This method is only invoked if the job is configured to use
     * the JMSDataWriter.
     *
     * @param record
     * @param jobId
     */
    @JmsListener(destination="datasponge.output.topic", containerFactory = "topicContainerFactory")
    public void handleOutputMessage(@Payload DataRecord record, @Header(JmsDataWriter.JOB_ID_PROP) String jobId){
        DataWriter writer = dataWriterMap.get(jobId);
        if(writer != null){
            writer.addItem(record);
        }
    }

    /**
     * intializes a JobExecutor for a crawl job.
     * @param jobId
     * @param nodeId
     * @param modSize
     * @param doSeed
     */
    protected void initializeEngineForJob(String jobId, int nodeId,
                                          int modSize, boolean doSeed) {

        JobExecutor executor = componentFactory.buildJobExecutor();
        executor.init(jobMap.get(jobId), nodeId, modSize, doSeed);
        jobExecutorMap.put(jobId, executor);
        executor.executeCrawl();
    }

    /**
     * sends the enrollment message on the management topic.
     * @param jobId
     */
    protected void sendEnrollment(final String jobId) {
        ManagementMessage msg = new ManagementMessage();
        msg.setType(Type.ENROLLMENT);
        msg.setJobId(jobId);
        Map<String, String> payload = new HashMap<String, String>();
        payload.put(HOST_KEY, HOST_ID);
        msg.setData(payload);
        managementTopicTemplate.send(buildMessageCreator(msg));
    }

    protected MessageCreator buildMessageCreator(final ManagementMessage msg) {
        return new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                try {
                    Message m = session.createTextMessage(mapper
                            .writeValueAsString(msg));

                    return m;
                } catch (Exception e) {
                    logger.error("Could not publish json message", e);
                    throw new JMSException("Could not publish message: "
                            + e.getMessage());
                }
            }
        };
    }

    /**
     * returns true if this node is the coordinator for the job identified by the id passed in.
     * @param jobId
     * @return
     */
    private boolean isJobCoordinator(String jobId) {
        if (jobId != null) {
            Job j = jobMap.get(jobId);
            if (j != null && j.getCoordinatorId() != null
                    && this.HOST_ID.equals(j.getCoordinatorId())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

}
