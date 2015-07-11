package org.cataractsoftware.datasponge.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cataractsoftware.datasponge.model.Job.Mode;
import org.cataractsoftware.datasponge.model.PluginConfig.Type;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ModelMarshallingTest {

    @Test
    public void testMarshall() {
        Job job = new Job();
        job.setJobName("TestJob");
        Set<String> set = new HashSet<String>();
        set.add(".*.rss");
        job.setIgnorePatterns(set);
        set = new HashSet<String>();
        set.add(".*www.buffalonews.com.*");
        job.setIncludePatterns(set);
        job.setMaxThreads(10);
        job.setMode(Mode.ONCE);
        PluginConfig conf = new PluginConfig();
        conf.setType(Type.DATA_EXTRACTOR);
        conf.setClassName("org.cataractsoftware.datasponge.extractor.HyperlinkExtractor");
        job.setDataExtractor(conf);
        conf = new PluginConfig();
        conf.setType(Type.DATA_WRITER);
        conf.setClassName("org.cataractsoftware.healthinspections.scraper.MongoDataWriter");
        Properties props = new Properties();
        props.setProperty("mongohost", "localhost");
        props.setProperty("mongouser", "username");
        props.setProperty("mongopw", "passwd");
        conf.setPluginProperties(props);
        job.setDataWriter(conf);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String val = mapper.writeValueAsString(job);
            assertTrue("Json string should not be empty", val.length() > 0);
            assertTrue("Json string should contain 'mongohost'",
                    val.contains("\"mongohost\""));
        } catch (Exception e) {
            assertTrue("Could not marshal object: " + e.getMessage(), false);
        }
    }

    @Test
    public void testUnmarshall() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Job job = mapper.readValue(new File(getClass().getClassLoader()
                    .getResource("samplejobConfig.json").getFile()), Job.class);
            assertTrue("plugin config not set", job.getDataWriter()
                    .getPluginProperties().get("jmsDestination") != null);
        } catch (Exception e) {
            assertTrue("Could not unmarshall: " + e.getMessage(), false);
        }
    }
}
