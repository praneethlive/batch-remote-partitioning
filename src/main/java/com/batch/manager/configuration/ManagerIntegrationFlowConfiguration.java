package com.batch.manager.configuration;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;

/**
 * Configuration class for managing the integration flows in a master profile.
 * <p>
 * This class handles outbound and inbound message flows between the master component and worker components.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("manager")
@Configuration
public class ManagerIntegrationFlowConfiguration {


    /**
     * Configure outbound flow (requests going to workers)
     * <p>
     * Creates and returns a DirectChannel for managing outbound requests.
     *
     * @return the DirectChannel instance for managing outbound requests
     */
    @Bean
    public DirectChannel managerRequestsMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    /**
     * Creates an IntegrationFlow for sending messages to an outbound channel using AMQP.
     *
     * @param amqpTemplate                  the AMQP template used for sending messages
     * @param managerRequestsMessageChannel the direct channel where the messages will be sent to
     * @return the created IntegrationFlow
     */
    @Bean
    public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate, DirectChannel managerRequestsMessageChannel) {
        return IntegrationFlow.from(managerRequestsMessageChannel)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests"))
                .get();
    }

    /**
     * Configure inbound flow (requests coming from workers)
     * <p>
     * Creates and returns a DirectChannel for managing inbound requests.
     *
     * @return the DirectChannel instance for managing inbound requests
     */
    @Bean
    public DirectChannel managerRepliesMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    /**
     * Creates an IntegrationFlow for receiving inbound messages from an AMQP (Advanced Message Queuing Protocol) channel.
     *
     * @param connectionFactory            the ConnectionFactory to establish the connection with the AMQP server
     * @param managerRepliesMessageChannel a DirectChannel to which the inbound messages should be sent
     * @return the created IntegrationFlow
     */
    @Bean
    public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory, DirectChannel managerRepliesMessageChannel) {
        return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "replies"))
                .channel(managerRepliesMessageChannel)
                .get();
    }
}
