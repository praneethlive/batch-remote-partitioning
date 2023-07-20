package com.batch.worker.configuration;

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
 * Configuration class for worker integration flow.
 * This class provides methods to configure inbound and outbound flows for worker requests and replies.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("worker")
@Configuration
public class WorkerIntegrationFlowConfiguration {

    /**
     * Configure inbound flow (requests coming from the manager)
     * <p>
     * Creates and returns a DirectChannel for managing inbound requests.
     *
     * @return the DirectChannel instance for managing inbound requests
     */
    @Bean
    public DirectChannel workerRequestsMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    /**
     * Configures the inbound flow for handling requests coming from the manager.
     * <p>
     * Creates and returns an IntegrationFlow instance that connects an AMQP inbound adapter to
     * a DirectChannel for processing incoming requests.
     *
     * @param connectionFactory            the AMQP connection factory to use for receiving messages
     * @param workerRequestsMessageChannel the DirectChannel to use for processing inbound requests
     * @return the IntegrationFlow instance for handling inbound requests
     */
    @Bean
    public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory, DirectChannel workerRequestsMessageChannel) {
        return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(workerRequestsMessageChannel)
                .get();
    }

    /**
     * Configure outbound flow (replies going to the manager)
     * <p>
     * Creates and returns a DirectChannel for managing outbound requests.
     *
     * @return the DirectChannel instance for managing outbound requests
     */
    @Bean
    public DirectChannel workerRepliesMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    /**
     * Configures the outbound flow for handling replies going to the manager.
     * <p>
     * Creates an IntegrationFlow for managing outbound requests.
     *
     * @param amqpTemplate                the AmqpTemplate for sending messages
     * @param workerRepliesMessageChannel the DirectChannel for worker replies
     * @return the IntegrationFlow instance for managing outbound requests
     */
    @Bean
    public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate, DirectChannel workerRepliesMessageChannel) {
        return IntegrationFlow.from(workerRepliesMessageChannel)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("replies"))
                .get();
    }
}
