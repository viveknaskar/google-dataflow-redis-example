package com.click.example.functions;

import com.click.example.constants.RedisFieldIndex;
import com.google.api.client.util.Strings;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class TransformingData extends DoFn<String, String[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformingData.class);

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String[]> out) {
        String[] fields = line.split(REGEX_LINE_SPLITTER_PIPE);
        StringBuilder reason = new StringBuilder();

        /**
         * Added validation for guid, firstname,lastname, postcode and dob as these are
         * mandatory fields. So these fields are missing in the file uploaded in the input GCS bucket
         * the complete line will be excluded.
         * Sample input file with missing mandatory fields is added with this repo - validationcheckmandatory.txt
         */
        if (!Strings.isNullOrEmpty(fields[RedisFieldIndex.GUID.getValue()]) &&
                !Strings.isNullOrEmpty(fields[RedisFieldIndex.PPID.getValue()]) &&
                !Strings.isNullOrEmpty(fields[RedisFieldIndex.FIRSTNAME.getValue()]) &&
                !Strings.isNullOrEmpty(fields[RedisFieldIndex.LASTNAME.getValue()]) &&
                !Strings.isNullOrEmpty(fields[RedisFieldIndex.DOB.getValue()]) &&
                !Strings.isNullOrEmpty(fields[RedisFieldIndex.POSTAL_CODE.getValue()])) {

            out.output(fields);

        } else {

            if (Strings.isNullOrEmpty(fields[RedisFieldIndex.GUID.getValue()])){
                reason.append("Validation failed due to GUID empty or null" );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.PPID.getValue()])) {
                reason.append("Validation failed due to PPID empty or null" );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.FIRSTNAME.getValue()])) {
                reason.append("Validation failed due to First Name empty or null" );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.LASTNAME.getValue()])) {
                reason.append("Validation failed due to Last Name empty or null" );
            }
            else if (Strings.isNullOrEmpty(fields[RedisFieldIndex.DOB.getValue()])) {
                reason.append("Validation failed due to DOB empty or null" );
            } else {
                reason.append("Validation failed due to Postal Code empty or null" );
            }

            LOGGER.error("Validation failed for line with GUID: " + fields[RedisFieldIndex.GUID.getValue()] +
                            " | PPID: " + fields[RedisFieldIndex.PPID.getValue()] +
                    " | Errors: " + reason.toString());

        }

    }

}
