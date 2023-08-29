package com.viveknaskar.functions;

import com.viveknaskar.constants.KeyPrefix;
import com.viveknaskar.constants.RedisFieldIndex;
import com.viveknaskar.constants.PipelineConstants;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ProcessingRecords extends DoFn<String[], KV<String, String>>{

    @ProcessElement
    public void processElement(@Element String[] fields, OutputReceiver<KV<String, String>> out) {

        if (fields[RedisFieldIndex.GUID.getValue()] != null) {

            out.output(KV.of(KeyPrefix.FIRSTNAME.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.FIRSTNAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.MIDDLENAME.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.MIDDLENAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.LASTNAME.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.LASTNAME.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.DOB.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.DOB.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.POSTAL_CODE.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.POSTAL_CODE.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            out.output(KV.of(KeyPrefix.GENDER.toString()
                    .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                    .concat(fields[RedisFieldIndex.GENDER.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            if (!(fields.length < 9 )) {

                out.output(KV.of(KeyPrefix.PHONE_NUMBER.toString()
                        .concat(PipelineConstants.REDIS_KEY_SEPARATOR)
                        .concat(fields[RedisFieldIndex.PHONE_NUMBER.getValue()]), fields[RedisFieldIndex.GUID.getValue()]));

            }

        }
    }
}
