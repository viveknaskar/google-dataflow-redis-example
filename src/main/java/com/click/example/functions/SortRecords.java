package com.click.example.functions;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class SortRecords extends DoFn<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortRecords.class);

    TupleTag<String> validRecords;
    TupleTag<String> invalidRecords;

    public SortRecords(TupleTag<String> validRecords,  TupleTag<String> invalidRecords) {
        this.validRecords = validRecords;
        this.invalidRecords = invalidRecords;
    }

    private Boolean isNullOrEmpty(String value) {
        return (value == null || value.isEmpty());
    }
    private Boolean isRecordValid(String recordLine) {
        if (isNullOrEmpty(recordLine)) {
            return false;
        }
        String[] fields = recordLine.split(REGEX_LINE_SPLITTER_PIPE);
        Boolean isRecordDirty = false;
        StringBuilder errorMessageBuilder = new StringBuilder(recordLine);
        errorMessageBuilder.append(" : is invalid!");
        if(fields!=null && fields.length > 0) {
            for (String field : fields) {
                if(isNullOrEmpty(field)) {
                    isRecordDirty = true;
                }
            }
        }
        if(isRecordDirty) {
            LOGGER.error(errorMessageBuilder.toString());
        }
        return !isRecordDirty;
    }

    @ProcessElement
    public void processElement(@Element String line, MultiOutputReceiver out) {
        if(isRecordValid(line)) {
            out.get(validRecords).output(line);
        } else {
            out.get(invalidRecords).output(line);
        }
    }


}