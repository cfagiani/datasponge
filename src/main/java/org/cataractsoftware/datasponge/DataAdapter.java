package org.cataractsoftware.datasponge;

import java.util.Properties;

/**
 * common interface for the pluggable components of the system. This defines the lifecycle methods that will be called by the framework.
 *
 * @author Christopher Fagiani
 */
public interface DataAdapter {

    /**
     * called by the framework immediately after instantiation. The properties passed in reflect the full contents of the property file used to configure the dataSponge program.
     *
     * @param props initialized property object containing all properties used to load the program
     */
    void init(Properties props);

    void setJobId(String jobId);
}
