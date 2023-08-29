package com.viveknaskar.functions;

import com.viveknaskar.constants.RedisFieldIndex;
import com.google.api.client.util.Strings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.viveknaskar.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

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
        StringBuilder reason = new StringBuilder();
        /**
         * Added validation for guid, firstname,lastname, postcode and dob as these are
         * mandatory fields. So these fields are missing in the file uploaded in the input GCS bucket
         * the complete line will be excluded.
         * Sample input file with missing mandatory fields is added with this repo - validationcheckmandatory.txt
         */
        if (Strings.isNullOrEmpty(fields[RedisFieldIndex.GUID.getValue()]) ||
                Strings.isNullOrEmpty(fields[RedisFieldIndex.PPID.getValue()]) ||
                Strings.isNullOrEmpty(fields[RedisFieldIndex.FIRSTNAME.getValue()]) ||
                Strings.isNullOrEmpty(fields[RedisFieldIndex.LASTNAME.getValue()]) ||
                Strings.isNullOrEmpty(fields[RedisFieldIndex.DOB.getValue()]) ||
                Strings.isNullOrEmpty(fields[RedisFieldIndex.POSTAL_CODE.getValue()])) {

            reason.append("Validation failed due to ");

            if (Strings.isNullOrEmpty(fields[RedisFieldIndex.GUID.getValue()])){
                reason.append("GUID empty or null," );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.PPID.getValue()])) {
                reason.append("PPID empty or null," );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.FIRSTNAME.getValue()])) {
                reason.append("First Name empty or null," );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.LASTNAME.getValue()])) {
                reason.append("Last Name empty or null," );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.DOB.getValue()])) {
                reason.append("DOB empty or null," );
            } else {
                reason.append("Postal Code empty or null," );
            }

            LOGGER.error("Validation failed for the line with GUID: " + fields[RedisFieldIndex.GUID.getValue()] +
                    " | Reason: " + reason.toString().replaceAll(",$", ""));
            return false;
        }
        return true;
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