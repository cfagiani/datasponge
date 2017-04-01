package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.AbstractDataAdapter;
import org.cataractsoftware.datasponge.DataRecord;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.Properties;

/**
 * Datawriter implementation that will publish data records to JMS.
 *
 * @author Christopher Fagiani
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JmsDataWriter extends AbstractDataAdapter implements DataWriter {

    public static final String DESTINATION_NAME_PROP = "jmsDestination";
    public static final String JOB_ID_PROP = "jobId";
    private String destination;
    @Resource(name = "outputJmsTemplate")
    private JmsTemplate outputJmsTemplate;

    @Override
    public void addItem(final DataRecord record) {
        outputJmsTemplate.send(destination, new MessageCreator() {
            @Override
            public ObjectMessage createMessage(Session session) throws JMSException {
                ObjectMessage message = session.createObjectMessage();
                message.setObject(record);
                message.setStringProperty(JOB_ID_PROP, getJobId());
                return message;
            }
        });
    }


    @Override
    public void flushBatch() {
        //no-op
    }


    @Override
    public void finish() {
        //no-op
    }

    @Override
    public void init(Properties props) {
        this.destination = props.getProperty(DESTINATION_NAME_PROP);
    }
}
