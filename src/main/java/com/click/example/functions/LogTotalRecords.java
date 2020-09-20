package com.click.example.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTotalRecords extends DoFn<Long, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogTotalRecords.class);

    @ProcessElement
    public void processElement(@Element Long totalCount) {

        if(totalCount!=null && totalCount > 0) {
            LOGGER.info("The total number of records processed: " + totalCount);
        } else {
            LOGGER.info("No Records are there");
        }

    }

}
