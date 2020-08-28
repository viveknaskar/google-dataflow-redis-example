
package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.PipelineConstants.REDIS_KEY_SEPARATOR;
import static com.click.example.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

public class StarterPipeline {

    public static interface WordCountOptions extends PipelineOptions {
        /**
         * Bucket where the text files are taken as input file
         * @return
         */
        @Description("Path of the file to read from")
        @Default.String("gs://cloud-dataflow-bucket-input/*.txt")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Memorystore/Redis instance host. Update with running memorystore host
         * @return
         */
        @Description("Redis host")
        @Default.String("127.0.0.1")
        String getRedisHost();
        void setRedisHost(String value);

        /**
         * Memorystore/Redis instance port. The default port for Redis is 6379
         * @return
         */
        @Description("Redis port")
        @Default.Integer(6379)
        Integer getRedisPort();
        void setRedisPort(Integer value);

    }

    public static void main(String[] args) {

        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String[]> recordSet =
                p.apply(
                        "ReadLines", TextIO.read().from(options.getInputFile()))
                          .apply(
                            "Transform Record",
                            ParDo.of(new DoFn<String, String[]>() {
                                private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

                                @ProcessElement
                                public void processElement(@Element String line, OutputReceiver<String[]> out) {
                                    LOGGER.info("Line content: " + line);
                                    String[] fields = line.split(REGEX_LINE_SPLITTER_PIPE);
                                    out.output(fields);
                                }
                        }));
                recordSet.apply(
                        "Processing Record",
                        ParDo.of(new DoFn<String[], KV<String, String>>() {

                            private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

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
