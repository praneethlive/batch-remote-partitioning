package com.batch.manager.joblauncher;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * JobLauncher is a configuration class used to create a remote partitioning job in a Spring Boot application.
 * <p>
 * The @Profile("master") annotation indicates that this class will be activated only when the "master" profile is active.
 * The @Configuration annotation specifies that this class is a configuration class.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("manager")
@Configuration
public class ManagerJobLauncher {

    /**
     * Creates a remote partitioning job.
     *
     * @param jobRepository the job repository used for managing the job
     * @param managerStep   the step used for managing the remote partitioning job
     * @return the remote partitioning job created
     */
    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository, Step managerStep) {
        return new JobBuilder("remotePartitioningJob", jobRepository)
                .start(managerStep)
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
