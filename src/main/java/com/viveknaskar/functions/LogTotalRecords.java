package com.viveknaskar.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTotalRecords extends DoFn<Long, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogTotalRecords.class);

    @ProcessElement
    public void processElement(@Element Long totalCount, OutputReceiver<String> out) {

        if(totalCount!=null && totalCount > 0) {
            out.output(" | The total number of records processed: " + totalCount);
        } else {
            LOGGER.info("No Records are there");
        }

    }

}
