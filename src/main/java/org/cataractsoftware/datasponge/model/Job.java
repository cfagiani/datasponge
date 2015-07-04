package org.cataractsoftware.datasponge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;
import java.util.Set;

/**
 * data structure representing a crawl job
 *
 * @author Christopher Fagiani
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {
    private String jobName;
    private Date submissionTime;
    private Set<String> startUrls;
    private Set<String> includePatterns;
    private Set<String> ignorePatterns;
    private int maxThreads;
    private PluginConfig dataExtractor;
    private PluginConfig dataWriter;
    private PluginConfig coordinatorDataWriter;
    private PluginConfig[] dataEnhancers;
    private Status status;
    private Mode mode;
    private String guid;
    private String coordinatorId;
    private Long continuousCrawlInterval;
    public Job() {
        submissionTime = new Date();
        status = Status.SUBMITTED;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    public Set<String> getStartUrls() {
        return startUrls;
    }

    public void setStartUrls(Set<String> startUrls) {
        this.startUrls = startUrls;
    }

    public Set<String> getIncludePatterns() {
        return includePatterns;
    }

    public void setIncludePatterns(Set<String> includePatterns) {
        this.includePatterns = includePatterns;
    }

    public Set<String> getIgnorePatterns() {
        return ignorePatterns;
    }

    public void setIgnorePatterns(Set<String> ignorePatterns) {
        this.ignorePatterns = ignorePatterns;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public PluginConfig getDataExtractor() {
        return dataExtractor;
    }

    public void setDataExtractor(PluginConfig dataExtractor) {
        this.dataExtractor = dataExtractor;
    }

    public PluginConfig getDataWriter() {
        return dataWriter;
    }

    public void setDataWriter(PluginConfig dataWriter) {
        this.dataWriter = dataWriter;
    }

    public PluginConfig[] getDataEnhancers() {
        return dataEnhancers;
    }

    public void setDataEnhancers(PluginConfig[] dataEnhancers) {
        this.dataEnhancers = dataEnhancers;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public void setCoordinatorId(String coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    public Long getContinuousCrawlInterval() {
        return continuousCrawlInterval;
    }

    public void setContinuousCrawlInterval(Long continuousCrawlInterval) {
        this.continuousCrawlInterval = continuousCrawlInterval;
    }

    public PluginConfig getCoordinatorDataWriter() {
        return coordinatorDataWriter;
    }

    public void setCoordinatorDataWriter(PluginConfig coordinatorDataWriter) {
        this.coordinatorDataWriter = coordinatorDataWriter;
    }

    public enum Status {
        SUBMITTED, PROCESSING, COMPLETE
    }

    public enum Mode {
        ONCE, CONTINUOUS
    }
}
