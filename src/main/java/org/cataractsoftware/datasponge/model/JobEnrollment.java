package org.cataractsoftware.datasponge.model;

/**
 * data structure used to have a server enroll in a crawl job
 *
 * @author Christopher Fagiani
 */
public class JobEnrollment {

    private String jobId;
    private String hostId;
    private long lastHeartbeat=0l;
    private boolean complete = false;

    public JobEnrollment() {

    }

    public JobEnrollment(String job, String host) {
        this.jobId = job;
        this.hostId = host;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostId == null) ? 0 : hostId.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JobEnrollment other = (JobEnrollment) obj;
        if (hostId == null) {
            if (other.hostId != null)
                return false;
        } else if (!hostId.equals(other.hostId))
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
            return false;
        return true;
    }

}
