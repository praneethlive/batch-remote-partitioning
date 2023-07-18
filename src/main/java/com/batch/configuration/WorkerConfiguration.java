package com.batch.configuration;

import com.batch.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Slf4j
@RequiredArgsConstructor
@Profile("worker")
@Configuration
public class WorkerConfiguration {

    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;

    /*
     * Configure inbound flow (requests coming from the manager)
     */
    @Bean
    public DirectChannel requests() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory, DirectChannel requests) {
        return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(requests)
                .get();
    }

    /*
     * Configure outbound flow (replies going to the manager)
     */
    @Bean
    public DirectChannel replies() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate, DirectChannel replies) {
        return IntegrationFlow.from(replies)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("replies"))
                .get();
    }

    /*
     * Configure the worker step
     */
    @Bean
    public Step workerStep(PlatformTransactionManager transactionManager, DirectChannel requests, DirectChannel replies,
                           FlatFileItemReader<Transaction> fileTransactionReader, JdbcBatchItemWriter<Transaction> writer) {
        return this.workerStepBuilderFactory.get("workerStep")
                .inputChannel(requests)
                .outputChannel(replies)
                .<Transaction, Transaction>chunk(1000, transactionManager)
                .reader(fileTransactionReader)
                .processor(transaction -> {
                    log.info("processing transaction = {}", transaction);
                    return transaction;
                })
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> fileTransactionReader(
            @Value("#{stepExecutionContext['file']}") Resource resource) {

        return new FlatFileItemReaderBuilder<Transaction>()
                .saveState(false)
//                .name("flatFileTransactionReader")
                .resource(resource)
                .delimited()
                .names("account", "amount", "timestamp")
                .fieldSetMapper(fieldSet -> new Transaction(
                        fieldSet.readString("account"),
                        fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"),
                        fieldSet.readBigDecimal("amount")))
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }
}

