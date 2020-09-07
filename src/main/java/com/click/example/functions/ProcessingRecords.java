package com.click.example.functions;

import com.click.example.constants.KeyPrefix;
import com.click.example.constants.RedisFieldIndex;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static com.click.example.constants.PipelineConstants.REDIS_KEY_SEPARATOR;

public class ProcessingRecords extends DoFn<String[], KV<String, String>>{

    @ProcessElement
    public void processElement(@Element String[] fields, OutputReceiver<KV<String, String>> out) {

        if (fields[RedisFieldIndex.GUID.getValue()] != null) {

            out.output(KV.of(KeyPrefix.FIRSTNAME.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.FIRSTNAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.MIDDLENAME.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.MIDDLENAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.LASTNAME.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.LASTNAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.DOB.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.DOB.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.POSTAL_CODE.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.POSTAL_CODE.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.GENDER.toString()
                    .concat(REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.GENDER.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            if (!(fields.length < 9 )) {

                out.output(KV.of(KeyPrefix.PHONE_NUMBER.toString()
                        .concat(REDIS_KEY_SEPARATOR)
                        .concat(fields[RedisFieldIndex.PHONE_NUMBER.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            }

        }
    }
}
