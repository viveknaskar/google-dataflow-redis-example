package com.click.example.functions;

import com.click.example.constants.KeyPrefix;
import com.click.example.constants.RedisFieldIndex;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static com.click.example.constants.PipelineConstants.REDIS_KEY_SEPARATOR;

public class ProcessingPPID extends DoFn<String[], KV<String, KV<String, String>>> {

    @ProcessElement
    public void processElement(@Element String[] fields, OutputReceiver<KV<String, KV<String, String>>> out) {

        if (fields[RedisFieldIndex.GUID.getValue()] != null && fields[RedisFieldIndex.PPID.getValue()] != null) {
            out.output(KV.of(KeyPrefix.GUID.toString()
                            .concat(REDIS_KEY_SEPARATOR).concat(fields[RedisFieldIndex.GUID.getValue()]),
                    KV.of(KeyPrefix.PPID.toString(), fields[RedisFieldIndex.PPID.getValue()])));
        }
    }
}
