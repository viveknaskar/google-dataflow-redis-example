package com.viveknaskar.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProcessDetails extends DoFn<String, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogProcessDetails.class);

    StringBuilder msg = new StringBuilder();
    @ProcessElement
    public void processElement(@Element String count) {
        if(count!=null && count.length() > 0) {
            msg.append(count);
            LOGGER.info("Processing completed: "+ msg);
        } else {
            msg.append(count);
            LOGGER.info("Processing failed.");
        }
    }
}
