package org.cataractsoftware.datasponge.engine;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.net.URI;

/**
 * Spring Configuration class for initializing embedded broker. This can be optionally included based on whether the system
 * is using a stand-alone broker or the embedded broker.
 *
 * @author Christopher Fagiani
 */
@Configuration
@Profile("broker")
public class BrokerConfig {

    @Bean
    public BrokerService brokerService() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("datasponge");
        broker.setPersistent(false);
        broker.setUseJmx(true);
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://0.0.0.0:61616"));
        broker.addConnector(connector);
        broker.start();
        return broker;
    }
}
