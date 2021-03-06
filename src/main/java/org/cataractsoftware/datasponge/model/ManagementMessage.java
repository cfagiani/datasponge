package org.cataractsoftware.datasponge.model;

import java.util.Map;

/**
 * data structure for messages used to control the datasponge ensemble
 *
 * @author Christopher Fagiani
 */
public class ManagementMessage {

    private Type type;
    private Map<String, String> data;
    private String jobId;
    private String senderHostId;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getSenderHostId() {
        return senderHostId;
    }

    public void setSenderHostId(String senderHostId) {
        this.senderHostId = senderHostId;
    }

    public enum Type {
        HEARTBEAT, ENROLLMENT, ASSIGNMENT, NODE_FAILURE,COMPLETE, ABORT
    }


}
