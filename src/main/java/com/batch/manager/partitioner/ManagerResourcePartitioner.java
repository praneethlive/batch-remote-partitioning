package com.batch.manager.partitioner;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;

/**
 * ManagerResourcePartitioner is a class used to create a MultiResourcePartitioner object for partitioning resources.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("manager")
@Configuration
public class ManagerResourcePartitioner {

    /**
     * Creates a {@link MultiResourcePartitioner} object with the provided resources.
     *
     * @param resources an array of resources to be partitioned
     * @return a {@link MultiResourcePartitioner} object with the provided resources
     */
    @Bean
    @StepScope
    public MultiResourcePartitioner partitioner(@Value("#{jobParameters['inputFiles']}") Resource[] resources) {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();

        partitioner.setKeyName("file");
        partitioner.setResources(resources);

        return partitioner;
    }

}
