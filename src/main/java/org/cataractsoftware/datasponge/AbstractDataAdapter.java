package org.cataractsoftware.datasponge;

/**
 * @author Christopher Fagiani
 */
public abstract class AbstractDataAdapter implements DataAdapter {
    private String jobId;

    public String getJobId() {
        return this.jobId;
    }

    @Override
    public void setJobId(String id) {
        this.jobId = id;
    }

}
