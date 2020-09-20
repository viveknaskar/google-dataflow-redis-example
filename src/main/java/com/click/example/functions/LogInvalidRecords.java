package com.click.example.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogInvalidRecords extends DoFn<Long, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogInvalidRecords.class);

    @DoFn.ProcessElement
    public void processElement(@Element Long count) {

        if(count!=null && count > 0) {
            LOGGER.info("Total Invalid Records: " + count);
        } else {
            LOGGER.info("No invalid Records");
        }

    }
}
