package org.cataractsoftware.datasponge.api;

import org.cataractsoftware.datasponge.engine.JobCoordinator;
import org.cataractsoftware.datasponge.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * RESTful interface for datasponge control plane
 *
 * @author Christopher Fagiani
 */
@RestController
@RequestMapping("/job")
public class JobController {

    @Autowired
    private JobCoordinator coordinator;

    /**
     * returns the status of the job indentified by the id
     * @param id
     * @return
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    @ResponseBody
    public String getJobStatus(@PathVariable("id") String id) {
        return coordinator.getJobStatus(id);
    }

    /**
     * submits a job for processing
     * @param job
     * @return
     */
    @RequestMapping(method = RequestMethod.POST)
    public String submitJob(@RequestBody Job job) {
        return coordinator.submitJob(job).getGuid();
    }
}
