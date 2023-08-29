package com.viveknaskar.functions;

import org.apache.beam.sdk.transforms.DoFn;

import static com.viveknaskar.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class TransformingData extends DoFn<String, String[]> {

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String[]> out) {
        String[] fields = line.split(REGEX_LINE_SPLITTER_PIPE);
        out.output(fields);
    }

}
