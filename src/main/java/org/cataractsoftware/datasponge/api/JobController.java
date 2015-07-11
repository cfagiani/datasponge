package org.cataractsoftware.datasponge.api;

import org.cataractsoftware.datasponge.engine.JobCoordinator;
import org.cataractsoftware.datasponge.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * RESTful interface for datasponge control plane
 *
 * @author Christopher Fagiani
 */
@RestController
@Profile("restapi")
@RequestMapping("/job")
public class JobController {

    @Autowired
    private JobCoordinator coordinator;

    /**
     * returns the status of the job indentified by the id
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    @ResponseBody
    public Job getJob(@PathVariable("id") String id) {
        return coordinator.getJob(id);
    }

    /**
     * submits a job for processing
     *
     * @param job
     * @return
     */
    @RequestMapping(method = RequestMethod.POST)
    public String submitJob(@RequestBody Job job) {
        return coordinator.submitJob(job).getGuid();
    }

    /**
     * returns all jobs
     * @param status
     * @return
     */
    @RequestMapping(method=RequestMethod.GET)
    public List<Job> getAllJobs(@RequestParam(value = "status", required = false)String status){
        return coordinator.getAllJobs(status != null?Job.Status.valueOf(status):null);
    }
}
