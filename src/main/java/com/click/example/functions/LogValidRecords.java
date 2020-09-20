package com.click.example.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogValidRecords extends DoFn<Long, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogValidRecords.class);

    @ProcessElement
    public void processElement(@Element Long count) {

        if(count!=null && count > 0) {
            LOGGER.info("Total Valid Records: " + count);
        } else {
            LOGGER.info("No valid Records");
        }

    }

}