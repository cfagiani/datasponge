package org.cataractsoftware.datasponge.support;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListenerAnnotationBeanPostProcessor;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * This Spring bean post processor is a hack to allow beans initialized after the application context has finished initializing
 * (for instance, singleton beans initialized in response to some system input) to be able to use the @JMSListener annotations.
 * The standard JmsListenerAnnotationBeanPostProcessor will only register the listeners when the Spring container calls
 * afterSingletonsInstantiated. This class will do nothing until then (since the default post processor is already present, it can handle
 * registering listeners until then) and then it will register and start any new listeners that are encountered by the container.
 *
 * @author Christopher Fagiani
 */
@Component
public class DynamicJmsListenerAnnotationBeanPostProcessor extends JmsListenerAnnotationBeanPostProcessor {
    @Autowired
    private JmsListenerEndpointRegistry registry;
    private volatile boolean initialSetupComplete = false;

    /**
     * force this instance to register all listeners it has encountered
     */
    public void refresh() {
        if (initialSetupComplete) {
            super.afterSingletonsInstantiated();
            if (registry.getListenerContainers() != null) {
                for (MessageListenerContainer container : registry.getListenerContainers()) {
                    if (!container.isRunning()) {
                        container.start();
                    }
                }
            }
            resetRegistrar();
        }
    }

    /**
     * resets the registrar; this method is a hack and could break upon upgrades to Spring since it uses reflection
     * to access private members. TODO: come up with better approach
     */
    private void resetRegistrar() {
        Field registrarField = ReflectionUtils.findField(JmsListenerAnnotationBeanPostProcessor.class, "registrar");
        ReflectionUtils.makeAccessible(registrarField);
        JmsListenerEndpointRegistrar registrar = (JmsListenerEndpointRegistrar) ReflectionUtils.getField(registrarField, this);
        Field descriptorField = ReflectionUtils.findField(JmsListenerEndpointRegistrar.class, "endpointDescriptors");
        ReflectionUtils.makeAccessible(descriptorField);
        List descriptorList = (List) ReflectionUtils.getField(descriptorField, registrar);
        descriptorList.clear();
    }


    /**
     * does nothing if initialSetupComplete is false. If true, it will call the postProcessAfterInitialization method of the parent class.
     *
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
        if (initialSetupComplete) {
            return super.postProcessAfterInitialization(bean, beanName);
        } else {
            return bean;
        }
    }

    /**
     * sets initialSetupComplete to true indicating that this class should start processing beans.
     */
    @Override
    public void afterSingletonsInstantiated() {
        initialSetupComplete = true;
    }

}
