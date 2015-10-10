package org.cataractsoftware.datasponge.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cataractsoftware.datasponge.model.ManagementMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Component used to create and send ManagementMessages to the coordination topic.
 */
@Component
public class ManagementMessageSender {
    private static final Logger logger = LoggerFactory
            .getLogger(ManagementMessageSender.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String NODE_KEY = "nodeId";
    public static final String SIZE_KEY = "modSize";
    public static final String HOST_ID = UUID.randomUUID().toString();



    @Resource(name = "managementTopicTemplate")
    private JmsTemplate managementTopicTemplate;


    /**
     * sends the enrollment message on the management topic.
     *
     * @param jobId
     */
    public void sendEnrollment(String jobId) {
        sendBasicMessage(jobId, ManagementMessage.Type.ENROLLMENT);
    }

    /**
     * sends the ASSIGNMENT message on the control topic to assign an ID to each participant.
     *
     * @param jobId
     * @param nodeId
     * @param modSize
     */
    public void sendAssignment(String jobId, int nodeId, int modSize) {
        ManagementMessage assignmentMessage = constructMessage(jobId, ManagementMessage.Type.ASSIGNMENT);
        Map<String, String> data = new HashMap<String, String>();
        data.put(NODE_KEY, nodeId + "");
        data.put(SIZE_KEY, modSize + "");
        assignmentMessage.setData(data);
        managementTopicTemplate.send(buildMessageCreator(assignmentMessage));
    }

    /**
     * sends a status management message on the control topic to broadcast proof of life for this node/job
     * @param jobId
     */
    protected void sendBasicMessage(String jobId, ManagementMessage.Type type){
        managementTopicTemplate.send(buildMessageCreator(constructMessage(jobId,type)));
    }

    /**
     * helper method to construct a base ManagementMessage object
     * @param jobId
     * @param type
     * @return
     */
    private ManagementMessage constructMessage(String jobId, ManagementMessage.Type type){
        ManagementMessage msg = new ManagementMessage();
        msg.setJobId(jobId);
        msg.setSenderHostId(HOST_ID);
        msg.setType(type);
        return msg;
    }

    /**
     * sends an ABORT message
     * @param jobId
     */
    public void sendAbort(String jobId){
        sendBasicMessage(jobId, ManagementMessage.Type.ABORT);
    }

    /**
     * sends heartbeat messages
     */
    public void sendHeartbeat(String jobId){
        sendBasicMessage(jobId, ManagementMessage.Type.HEARTBEAT);
    }

    /**
     * sends complete message
     * @param jobId
     */
    public void sendComplete(String jobId){
        sendBasicMessage(jobId, ManagementMessage.Type.COMPLETE);
    }

    /**
     * sends a message indicating hostId failed for the job identified by jobId
     * @param jobId
     * @param hostId
     */
    public void sendFailure(String jobId, String hostId){
        ManagementMessage failureMessage = constructMessage(jobId, ManagementMessage.Type.NODE_FAILURE);
        Map<String, String> data = new HashMap<String, String>();
        data.put(NODE_KEY, hostId + "");
        failureMessage.setData(data);
        managementTopicTemplate.send(buildMessageCreator(failureMessage));
    }

    /**
     * builds a SpringJMS MessageCreator for use with the JMSTemplate when sending messages.
     * @param msg
     * @return
     */
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

}
