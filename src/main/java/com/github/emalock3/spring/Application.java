package com.github.emalock3.spring;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.springframework.integration.jms.JmsOutboundGateway;
import org.springframework.integration.jms.config.JmsChannelFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.support.PeriodicTrigger;

@Configuration
@ComponentScan
@EnableIntegration
@EnableAutoConfiguration
public class Application {
    
    private static final String QUEUE_NAME = "test.queue";
    
    @Bean(destroyMethod = "stop")
    public PooledConnectionFactory jmsConnFactory() {
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
    public JmsOutboundGateway pushQueueGateway(ConnectionFactory jmsConnFactory) {
        JmsOutboundGateway gateway = new JmsOutboundGateway();
        gateway.setConnectionFactory(jmsConnFactory);
        gateway.setRequestDestinationName(QUEUE_NAME);
        return gateway;
    }
    
    @Bean(destroyMethod = "destroy")
    public JmsChannelFactoryBean jmsInboundChannel(ConnectionFactory jmsConnFactory) {
        JmsChannelFactoryBean factory = new JmsChannelFactoryBean(true);
        factory.setConnectionFactory(jmsConnFactory);
        factory.setSessionTransacted(true);
        factory.setDestinationName(QUEUE_NAME);
        return factory;
    }
    
    private final AtomicInteger counter = new AtomicInteger();
    
    @Bean
    @ServiceActivator(inputChannel = "jmsInboundChannel")
    public MessageHandler loggingHandler() {
        return message -> {
            String now = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
            if (counter.incrementAndGet() % 3 == 0) {
                throw new RuntimeException("TEST!: " + message.getPayload());
            }
            System.out.println(now + ": " + message.getPayload());
        };
    }
    
    public static void main(String ... args) {
        new SpringApplicationBuilder(Application.class)
                .showBanner(false)
                .run(args);
    }
}
