package com.click.example.functions;

import com.click.example.StarterPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class TransformingDataFunction extends DoFn<String, String[]> {

    private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String[]> out) {
        LOGGER.info("Line content: " + line);
        String[] fields = line.split(REGEX_LINE_SPLITTER_PIPE);
        out.output(fields);
    }

}
