package com.click.example.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountTotalRecords extends DoFn<Long, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountTotalRecords.class);

    @ProcessElement
    public void processElement(@Element Long totalCount) {

        LOGGER.info("The total number of records processed: " + totalCount);

    }

}
