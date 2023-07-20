package com.batch.worker.reader;

import com.batch.domain.Transaction;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;

/**
 * This class represents a FlatFileReader that can be used to read transactions from a given resource.
 * It is annotated with @Profile("worker") and @Configuration.
 * The FlatFileReader is responsible for creating a FlatFileItemReader that reads transactions from a file.
 * <p>
 *
 * @author Praneeth Jayawardena
 * @version 2.0
 * @since v2.0
 */
@Profile("worker")
@Configuration
public class FlatFileReader {

    /**
     * Creates a FlatFileItemReader for reading transactions from a given resource.
     *
     * @param resource the resource to read transactions from
     * @return the FlatFileItemReader instance for reading transactions from a file
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> fileTransactionReader(
            @Value("#{stepExecutionContext['file']}") Resource resource) {

        return new FlatFileItemReaderBuilder<Transaction>()
                .saveState(false)
                .name("flatFileTransactionReader")
                .resource(resource)
                .delimited()
                .names("account", "amount", "timestamp")
                .fieldSetMapper(fieldSet -> new Transaction(
                        fieldSet.readString("account"),
                        fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"),
                        fieldSet.readBigDecimal("amount")))
                .build();
    }
}
