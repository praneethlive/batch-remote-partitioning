package com.batch.manager.jobconfig;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.integration.channel.DirectChannel;

import java.util.Objects;

/**
 * The MasterJobConfiguration class is responsible for configuring and creating a step for the manager in a master-worker job scenario.
 * This class should be used only when running in a master profile.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@RequiredArgsConstructor
@Profile("manager")
@Configuration
public class ManagerJobConfiguration {

    private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;
    private final Environment env;


    /**
     * Creates a step for the manager.
     *
     * @param partitioner                   the partitioner used for partitioning the step
     * @param managerRequestsMessageChannel the channel for sending requests to the manager
     * @param managerRepliesMessageChannel  the channel for receiving replies from the manager
     * @return the step created for the manager
     */
    @Bean
    public Step managerStep(MultiResourcePartitioner partitioner, DirectChannel managerRequestsMessageChannel, DirectChannel managerRepliesMessageChannel) {
        return managerStepBuilderFactory.get("managerStep")
                .partitioner("workerStep", partitioner)
                .gridSize(Integer.parseInt(Objects.requireNonNull(env.getProperty("worker.node.count")))) // == Worker Nodes Count ==
                .outputChannel(managerRequestsMessageChannel)
                .inputChannel(managerRepliesMessageChannel)
                .build();
    }
}
