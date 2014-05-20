package com.github.emalock3.spring;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.jms.JmsDestinationPollingSource;
import org.springframework.integration.jms.JmsOutboundGateway;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.support.PeriodicTrigger;

@Configuration
@ComponentScan
@EnableIntegration
@EnableAutoConfiguration
public class Application {
    
    @Bean(destroyMethod = "stop")
    public PooledConnectionFactory jmsFactory() {
        return new PooledConnectionFactory(
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"));
    }
    
    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(60L, TimeUnit.SECONDS));
        return pollerMetadata;
    }
    
    @Bean
    public MessageChannel requestPushChannel() {
        return new DirectChannel();
    }
    
    @Bean
    @InboundChannelAdapter(value = "requestPushChannel", 
            poller = @Poller(fixedRate = "10000", maxMessagesPerPoll = "1"))
    public MessageSource<String> testSource() {
        return () -> {
            String date = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            return MessageBuilder.withPayload(date).build();
        };
    }
    
    @Bean
    @ServiceActivator(inputChannel = "requestPushChannel")
    public JmsOutboundGateway pushQueueGateway(ConnectionFactory connFactory) {
        JmsOutboundGateway gateway = new JmsOutboundGateway();
        gateway.setConnectionFactory(connFactory);
        gateway.setRequestDestinationName("test.queue");
        return gateway;
    }
    
    @Bean
    public MessageChannel requestPopChannel() {
        return new DirectChannel();
    }
    
    @Bean
    @InboundChannelAdapter(value = "requestPopChannel", 
            poller = @Poller(fixedDelay = "1000", maxMessagesPerPoll = "10"))
    public JmsDestinationPollingSource jmsQueueSource(JmsTemplate jmsTemplate) {
        JmsDestinationPollingSource source = new JmsDestinationPollingSource(jmsTemplate);
        source.setDestinationName("test.queue");
        return source;
    }
    
    @Bean
    @ServiceActivator(inputChannel = "requestPopChannel")
    public LoggingHandler loggingHandler() {
        return new LoggingHandler("info");
    }
    
    public static void main(String ... args) {
        new SpringApplicationBuilder(Application.class)
                .showBanner(false)
                .run(args);
    }
}
