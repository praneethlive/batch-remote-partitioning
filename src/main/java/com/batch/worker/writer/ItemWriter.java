package com.batch.worker.writer;

import com.batch.domain.Transaction;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

/**
 * This class is responsible for creating a JdbcBatchItemWriter for writing transactions to a database table.
 * <p>
 * It is annotated with the @Profile("worker") annotation to indicate that it should be used in a worker profile.
 * It is also annotated with the @Configuration annotation to indicate that it is a configuration class.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("worker")
@Configuration
public class ItemWriter {

    /**
     * Creates a JdbcBatchItemWriter for writing transactions to a database table.
     *
     * @param dataSource the data source to write transactions to
     * @return the JdbcBatchItemWriter instance for writing transactions to a database
     */
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
