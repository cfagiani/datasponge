package org.cataractsoftware.datasponge.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cataractsoftware.datasponge.DataRecord;
import org.cataractsoftware.datasponge.model.Job;
import org.cataractsoftware.datasponge.model.JobEnrollment;
import org.cataractsoftware.datasponge.model.ManagementMessage;
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

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.*;
import java.util.Map.Entry;

/**
 * This component is responsible for coordination of any jobs. If running in a multi-node setup, there could be multiple coordinators
 * in a single ensemble. In that case, the coordinator on the node to which the job was submitted is responsible for that job.
 */
@Component
public class JobCoordinator {
    private static final Logger logger = LoggerFactory
            .getLogger(JobCoordinator.class);

    private static final long OUTPUT_FLUSH_INTERVAL = 10000;
    private static final long FAILURE_INTERVAL = 60000;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int WAIT_TIME_SECS = 10;


    @Resource(name = "jobTopicTemplate")
    private JmsTemplate jobTopicTemplate;

    @Autowired
    private ManagementMessageSender managementMessageSender;

    @Autowired
    private ComponentFactory componentFactory;

    private volatile Map<String, Job> jobMap = new HashMap<String, Job>();
    private volatile Map<String, DataWriter> dataWriterMap = new HashMap<String, DataWriter>();
    private volatile Map<String, JobExecutor> jobExecutorMap = new HashMap<String, JobExecutor>();
    private volatile Map<String, List<JobEnrollment>> enrollmentMap = new HashMap<String, List<JobEnrollment>>();
    private Timer jobProgressTimer;

    public JobCoordinator() {
        jobProgressTimer = new Timer();
        jobProgressTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                updateJobStatus();
            }
        }, OUTPUT_FLUSH_INTERVAL, OUTPUT_FLUSH_INTERVAL);
    }



    /**
     * returns the Job domain object for the given guid (or null if not present)
     *
     * @param guid
     * @return
     */
    public Job getJob(String guid){
        return jobMap.get(guid);
    }

    /**
     * returns all jobs, optionally filtered by a status
     * @return
     */
    public List<Job> getAllJobs(Job.Status status){
        List<Job> jobs = new ArrayList<>();
        if(status != null) {
            for (Job j : jobMap.values()) {
                if(status == j.getStatus()){
                    jobs.add(j);
                }
            }
        }else {
            jobs.addAll(jobMap.values());
        }
        return jobs;
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
            j.setCoordinatorId(ManagementMessageSender.HOST_ID);
            if (j.getCoordinatorDataWriter() != null) {
                dataWriterMap.put(j.getGuid(), (DataWriter) componentFactory.getNewDataAdapter(j.getGuid(), j.getCoordinatorDataWriter()));
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
            managementMessageSender.sendEnrollment(j.getGuid());
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    synchronized(enrollmentMap) {
                        List<JobEnrollment> enrollments = enrollmentMap.get(j
                                .getGuid());
                        if (enrollments != null) {
                            for (int i = 0; i < enrollments.size(); i++) {
                                managementMessageSender.sendAssignment(j.getGuid(), i, enrollments.size());
                            }
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
    protected void updateJobStatus() {
        flushOutput();
        checkLocalCompletion();
        checkGlobalCompletion();
        checkForFailures();
    }

    /**
     * if on the job coordinator, checks for any nodes with the lastHearbeat > FAILURE_INTERVAL
     */
    protected synchronized void checkForFailures(){
        for(Entry<String,List<JobEnrollment>> enrollmentEntry: enrollmentMap.entrySet()){
            if(isJobCoordinator(enrollmentEntry.getKey())) {
                for (JobEnrollment enrollment : enrollmentEntry.getValue()) {
                    if(enrollment.getLastHeartbeat()> 0) {
                        if(System.currentTimeMillis() - enrollment.getLastHeartbeat() > FAILURE_INTERVAL){
                            managementMessageSender.sendFailure(enrollment.getJobId(),enrollment.getHostId());
                        }
                    }
                }
            }
        }
    }


    /**
     * checks if the local executor for a job is complete. If so, it sends a complete management message, otherwise it sends
     * a heartbeat message. If the executor is complete, the executor bean will be destroyed.
     */
    protected void checkLocalCompletion(){
        List<String> completedJobs = new ArrayList<String>();
        for (Map.Entry<String, JobExecutor> executorEntry : jobExecutorMap.entrySet()) {
            String jobId = executorEntry.getKey();
            if (executorEntry.getValue().isDone()) {
                completedJobs.add(jobId);
                managementMessageSender.sendComplete(jobId);
            } else {
                managementMessageSender.sendHeartbeat(jobId);
            }
        }
        if (completedJobs.size() > 0) {
            for (String jobId : completedJobs) {
                jobExecutorMap.remove(jobId);
                logger.info("Removed executor for job " + jobId);
                Job job = jobMap.get(jobId);
                if (job != null) {
                    job.setStatus(Job.Status.NODE_COMPLETE);
                }
                //TODO: need to unregister the JMS listeners

            }
        }
    }


    /**
     * checks if we have recived COMPLETE messages for all enrollments for a job. If so, the job is marked complete.
     * For jobs with a coordinatorDataWriter configured, the finish method will be called on the writer upon executor completion
     */
    protected synchronized void checkGlobalCompletion(){
        List<String> completedJobs = new ArrayList<String>();
       for(Map.Entry<String,List<JobEnrollment>> enrollmentEntry: enrollmentMap.entrySet()){
            if(enrollmentEntry.getValue()!=null){
                boolean allComplete = true;
                for(JobEnrollment e: enrollmentEntry.getValue()){
                    if(!e.isComplete()){
                        allComplete = false;
                        break;
                    }
                }
                if(allComplete) {
                    //if we're here, then all nodes have reported completion;
                    DataWriter writer = dataWriterMap.get(enrollmentEntry.getKey());
                    if (writer != null) {
                        writer.finish();
                        dataWriterMap.remove(enrollmentEntry.getKey());
                    }
                    Job job = jobMap.get(enrollmentEntry.getKey());
                    if (job != null) {
                        job.setStatus(Job.Status.COMPLETE);
                    }
                    //can also clean up enrollment map
                    completedJobs.add(enrollmentEntry.getKey());
                }
            }

        }
        if(completedJobs.size()>0){
            for(String jobId: completedJobs){
                enrollmentMap.remove(jobId);
            }
        }
    }


    /**
     * iterates over all dataWriters and flushes their output
     */
    protected void flushOutput() {
        for (DataWriter writer : dataWriterMap.values()) {
            writer.flushBatch();
        }
    }



    /**
     * respond to job messages by enrolling.
     *
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
                managementMessageSender.sendEnrollment(job.getGuid());
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
                            Integer.parseInt(msg.getData().get(ManagementMessageSender.NODE_KEY)),
                            Integer.parseInt(msg.getData().get(ManagementMessageSender.SIZE_KEY)),
                            isJobCoordinator(msg.getJobId()));
                    break;
                case HEARTBEAT:
                    updateEnrollment(msg, false);
                    break;
                case ABORT:
                    handleAbort(msg.getJobId());
                    break;
                case COMPLETE:
                    updateEnrollment(msg,true);
                    break;
                case NODE_FAILURE:
                    handleFailure(msg.getJobId(),Integer.parseInt(msg.getData().get(ManagementMessageSender.NODE_KEY)));
                    break;
                default:
                    logger.error("Unknown control message type: "+msg.getType());
                    break;

            }
        } catch (Exception e) {
            throw new RuntimeException("Could not process job message", e);
        }
    }

    /**
     * handles the failure of a node by updating the executor so it can adjust its share of the workqueue
     * @param jobId
     * @param nodeId
     */
    protected void handleFailure(String jobId, int nodeId){
        JobExecutor executor = jobExecutorMap.get(jobId);
        if(executor!=null){
           if(executor.handleNodeFailure(nodeId)){
               //if the node that failed is this node,then the coordinator must not be getting our messages so we can terminates
               handleAbort(jobId);
           }
        }
    }


    /**
     * handles an abort by shutting down the executor for the job and flushing anything pending
     * @param msg
     */
    protected synchronized void handleAbort(String jobId){
        JobExecutor executor = jobExecutorMap.get(jobId);

        if(executor != null){
            logger.info("Aborting job "+jobId);
            executor.destroy();
            jobExecutorMap.remove(jobId);
        }
        if(dataWriterMap.get(jobId) !=null){
            dataWriterMap.get(jobId).finish();
            dataWriterMap.remove(jobId);
        }
        Job j = jobMap.get(jobId);
        if(j != null){
            j.setStatus(Job.Status.ABORTED);
        }
    }

    /**
     * updates the heartbeat timestamp on the enrollment
     * @param msg
     */
    protected void updateEnrollment(ManagementMessage msg, boolean isComplete){
        String jobId = msg.getJobId();
        List<JobEnrollment> enrollments = enrollmentMap.get(jobId);
        if(enrollments != null){
            for(JobEnrollment e: enrollments){
                if(ManagementMessageSender.HOST_ID.equals(e.getHostId())){
                    if(isComplete){
                        e.setComplete(true);
                    }else {
                        e.setLastHeartbeat(System.currentTimeMillis());
                    }
                }
            }
        }
    }

    /**
     * records an enrollment
     *
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
            JobEnrollment enrollment = new JobEnrollment(jobId, msg.getSenderHostId());
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
    @JmsListener(destination = "datasponge.output.topic", containerFactory = "topicContainerFactory")
    public void handleOutputMessage(@Payload DataRecord record, @Header(JmsDataWriter.JOB_ID_PROP) String jobId) {
        DataWriter writer = dataWriterMap.get(jobId);
        if (writer != null) {
            writer.addItem(record);
        }
    }

    /**
     * intializes a JobExecutor for a crawl job.
     *
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
     * returns true if this node is the coordinator for the job identified by the id passed in.
     *
     * @param jobId
     * @return
     */
    private boolean isJobCoordinator(String jobId) {
        if (jobId != null) {
            Job j = jobMap.get(jobId);
            if (j != null && j.getCoordinatorId() != null
                    && ManagementMessageSender.HOST_ID.equals(j.getCoordinatorId())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }


    /**
     * checks if all jobs submitted to this coordinator are complete and returns true if so.
     * If NO jobs have yet to be submitted to this coordinator, this method will always return false.
     *
     * @return
     */
    public boolean areAllJobsDone() {
        if (jobMap.size() > 0) {
            for (Map.Entry<String, Job> jobEntry : jobMap.entrySet()) {
                if (Job.Status.COMPLETE != jobEntry.getValue().getStatus() && Job.Status.ABORTED != jobEntry.getValue().getStatus()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * sends the abort control message on the management topic
     * @param jobId
     * @return
     */
    public boolean abortJob(String jobId){
        if(jobMap.get(jobId)!=null){
            managementMessageSender.sendAbort(jobId);
            return true;
        }else{
            return false;
        }
    }

    @PreDestroy
    public void destroy() {
        if (jobProgressTimer != null) {
            jobProgressTimer.cancel();
        }
    }

}
