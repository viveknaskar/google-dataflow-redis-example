package com.click.example;

import com.click.example.constants.KeyPrefix;
import com.click.example.constants.RedisFieldIndex;
import com.click.example.functions.CustomRedisIODoFun;
import com.click.example.functions.TransformingDataFunction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.constants.PipelineConstants.REDIS_KEY_SEPARATOR;

public class StarterPipeline {

    public static void main(String[] args) {
        /**
         * Constructed StorageToRedisOptions object using the method PipelineOptionsFactory.fromArgs
         * to read options from command-line
         */
        StorageToRedisOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(StorageToRedisOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String[]> recordSet =
                p.apply(
                        "ReadLines", TextIO.read().from(options.getInputFile()))
                          .apply(
                            "Transform Record",
                            ParDo.of(new TransformingDataFunction()));
                recordSet.apply(
                        "Processing Record",
                        ParDo.of(new DoFn<String[], KV<String, String>>() {

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

                                }
                            }
                        })).apply("Writing field indexes into redis",
                        RedisIO.write().withMethod(RedisIO.Write.Method.SADD)
                                .withEndpoint(options.getRedisHost(), options.getRedisPort()));

        recordSet.apply(
                "Processing Payroll Provider ID",
                ParDo.of(new DoFn<String[], KV<String, KV<String, String>>>() {

                    private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

                    @ProcessElement
                    public void processElement(@Element String[] fields, OutputReceiver<KV<String, KV<String, String>>> out) {

                        if (fields[RedisFieldIndex.GUID.getValue()] != null && fields[RedisFieldIndex.PPID.getValue()] != null) {
                            out.output(KV.of(KeyPrefix.GUID.toString()
                                    .concat(REDIS_KEY_SEPARATOR).concat(fields[RedisFieldIndex.GUID.getValue()]),
                                    KV.of(KeyPrefix.PPID.toString(), fields[RedisFieldIndex.PPID.getValue()])));
                            LOGGER.info("Created the hash!");
                        }
                    }
                })).apply("Writing Hash Data into Redis",
                ParDo.of(new CustomRedisIODoFun(options.getRedisHost(), options.getRedisPort())));
        p.run();
    }
}
