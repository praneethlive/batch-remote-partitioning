package com.batch;

import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@EnableBatchIntegration
@SpringBootApplication
public class BatchRemotePartitioningApplication {

    public static void main(String[] args) {

        List<String> strings = Arrays.asList(args);

        List<String> finalArgs = new ArrayList<>(strings.size() + 1);
        finalArgs.addAll(strings);
        finalArgs.add("inputFiles=/data/csv/transactions*.csv");

        SpringApplication.run(BatchRemotePartitioningApplication.class, finalArgs.toArray(new String[0]));
    }

}
