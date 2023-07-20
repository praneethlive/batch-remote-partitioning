package com.batch.worker.stepconfig;

import com.batch.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * WorkerStepConfig class is responsible for configuring the worker step in the data processing workflow.
 * It creates a Step for processing transactions in chunks using remote partitioning.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Slf4j
@RequiredArgsConstructor
@Profile("worker")
@Configuration
public class WorkerStepConfig {

    private final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

    /**
     * Configures the worker step in the data processing workflow.
     * <p>
     * Creates a Step for processing transactions in chunks.
     *
     * @param transactionManager           the PlatformTransactionManager for managing transactions
     * @param workerRequestsMessageChannel the DirectChannel for worker requests
     * @param workerRepliesMessageChannel  the DirectChannel for worker replies
     * @param fileTransactionReader        the FlatFileItemReader for reading transactions from a file
     * @param writer                       the JdbcBatchItemWriter for writing transactions to a database
     * @return the Step instance for processing transactions in chunks
     */
    @Bean
    public Step workerStep(PlatformTransactionManager transactionManager,
                           DirectChannel workerRequestsMessageChannel,
                           DirectChannel workerRepliesMessageChannel,
                           FlatFileItemReader<Transaction> fileTransactionReader,
                           JdbcBatchItemWriter<Transaction> writer) {
        return remotePartitioningWorkerStepBuilderFactory.get("workerStep")
                .inputChannel(workerRequestsMessageChannel)
                .outputChannel(workerRepliesMessageChannel)
                .<Transaction, Transaction>chunk(1000, transactionManager)
                .reader(fileTransactionReader)
                .processor(transaction -> {
                    log.info("processing transaction = {}", transaction);
                    return transaction;
                })
                .writer(writer)
                .build();
    }
}
