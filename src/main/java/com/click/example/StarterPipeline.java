
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

import java.util.HashMap;

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
                                    String[] fields = line.split("\\|");
                                    out.output(fields);
                                }
                        }));
                recordSet.apply(
                        "Processing Record",
                        ParDo.of(new DoFn<String[], KV<String, String>>() {

                            private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

                            @ProcessElement
                            public void processElement(@Element String[] fields, OutputReceiver<KV<String, String>> out) {

                                HashMap<String, String> hmap = new HashMap<String, String>();
                                hmap.put("guid", fields[1]);
                                hmap.put("firstName", fields[2]);
                                hmap.put("middleName", fields[3]);
                                hmap.put("lastName", fields[4]);
                                hmap.put("dob", fields[5]);
                                hmap.put("postalCode", fields[6]);
                                hmap.put("gender", fields[7]);

                                if (fields[1] != null) {
                                    out.output(KV.of("hash1:".concat(hmap.get("firstName")), hmap.get("guid")));
                                    out.output(KV.of("hash2:".concat(hmap.get("middleName")), hmap.get("guid")));
                                    out.output(KV.of("hash3:".concat(hmap.get("lastName")), hmap.get("guid")));
                                    out.output(KV.of("hash4:".concat(hmap.get("dob")), hmap.get("guid")));
                                    out.output(KV.of("hash5:".concat(hmap.get("postalCode")), hmap.get("guid")));
                                    out.output(KV.of("hash6:".concat(hmap.get("gender")), hmap.get("guid")));
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
                        HashMap<String, String> hmap = new HashMap<String, String>();
                        hmap.put("ppid", fields[0]);
                        hmap.put("guid", fields[1]);

                        if (fields[0] != null && fields[1] != null) {
                            out.output(KV.of("hash11:".concat(hmap.get("guid")), KV.of("hash12", hmap.get("ppid"))));
                            LOGGER.info("Created the hash!");
                        }
                    }
                })).apply("Writing Hash Data into Redis",
                ParDo.of(new CustomRedisIODoFun(options.getRedisHost(), options.getRedisPort())));
        p.run();
    }
}
