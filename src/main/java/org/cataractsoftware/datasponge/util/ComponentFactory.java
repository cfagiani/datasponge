package org.cataractsoftware.datasponge.util;

import org.cataractsoftware.datasponge.DataAdapter;
import org.cataractsoftware.datasponge.engine.JobExecutor;
import org.cataractsoftware.datasponge.model.PluginConfig;
import org.cataractsoftware.datasponge.support.DynamicJmsListenerAnnotationBeanPostProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * utility class for constructing job-specific components
 *
 * @author Christopher Fagiani
 */
@Component
public class ComponentFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;
    /**
     * helper method to reflectively instantiate and initialize the pluggable
     * DataAdapter components (DataExtractor and DataWriter instances).
     *
     * @param adapterConf adapter configuration object
     * @param <T>         subtype of DataAdapter
     * @return initialized instance of T
     */
    @SuppressWarnings("unchecked")
    public <T extends DataAdapter> T getNewDataAdapter(String jobId, PluginConfig adapterConf) {
        if (adapterConf != null && adapterConf.getClassName() != null
                && adapterConf.getClassName().trim().length() > 0) {
            try {
                Class writerClass = Class.forName(adapterConf.getClassName());
                T adapter = null;
                try {
                    adapter = (T) applicationContext.getBean(writerClass);
                }catch(NoSuchBeanDefinitionException ex){
                    //try to instantiate as non-spring class
                    adapter = (T)writerClass.newInstance();
                }
                adapter.init(adapterConf.getPluginProperties());
                adapter.setJobId(jobId);
                return adapter;
            } catch (ReflectiveOperationException ex) {
                throw new RuntimeException(
                        "Could not instantiate "
                                + adapterConf.getClassName()
                                + " as DataAdapter. Ensure "
                                + adapterConf.getType()
                                + " property is a fully qualified class name and it is on the classpath",
                        ex);
            }
        } else {
            return null;
        }
    }

    /**
     * helper to initialize an array of DataAdapter components. It is assumed
     * that the fully-qualified class names of each component is passed in a
     * comma-delimited string via the pipelineClasses argument.
     *
     * @param pipelineStages array of PluginConfig objects describing the data pipeline
     * @return array of T
     */
    public <T extends DataAdapter> T[] getNewDataAdapterPipeline(String jobId,
            PluginConfig[] pipelineStages) {
        T[] adapters = null;
        if (pipelineStages != null) {
            for (PluginConfig conf : pipelineStages) {
                List<T> pipeline = new ArrayList<T>();
                T item = getNewDataAdapter(jobId,conf);
                if (item != null) {
                    pipeline.add(item);
                }
                adapters = pipeline.toArray(adapters);
            }
        }
        return adapters;
    }

    public JobExecutor buildJobExecutor() {
        JobExecutor executor = applicationContext
                .getBean(JobExecutor.class);
        DynamicJmsListenerAnnotationBeanPostProcessor proc = applicationContext.getBean(DynamicJmsListenerAnnotationBeanPostProcessor.class);
        proc.refresh();
        return executor;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
