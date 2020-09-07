package com.click.example.functions;

import com.click.example.constants.RedisFieldIndex;
import com.google.api.client.util.Strings;
import org.apache.beam.sdk.transforms.DoFn;

import static com.click.example.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class TransformingData extends DoFn<String, String[]> {

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String[]> out) {
        String[] fields = line.split(REGEX_LINE_SPLITTER_PIPE);
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
        }
    }

}
