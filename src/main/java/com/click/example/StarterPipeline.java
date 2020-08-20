/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class StarterPipeline {
//  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://cloud-dataflow-bucket-input/*.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Redis host")
        @Default.String("127.0.0.1")
        String getRedisHost();

        void setRedisHost(String value);

        @Description("redis port")
        @Default.Integer(6379)
        Integer getRedisPort();

        void setRedisPort(Integer value);

    }

    public static void main(String[] args) {

        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountOptions.class);

        Pipeline p = Pipeline.create(options);
//        PipelineOptionsFactory.fromArgs(args).withValidation().create());

        PCollection<String[]> recordSet = p.apply(
                "ReadLines", TextIO.read().from(options.getInputFile())).apply(
                "Transform Record",   // the transform name
                ParDo.of(new DoFn<String, String[]>() {    // a DoFn as an anonymous inner class instance

                    private final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

                    @ProcessElement
                    public void processElement(@Element String line, OutputReceiver<String[]> out) {
                        LOG.info("line content: " + line);
                        String[] fields = line.split("\\|");
                        out.output(fields);
                    }
                }));
        recordSet.apply(
                "Processing Record",                     // the transform name
                ParDo.of(new DoFn<String[], KV<String, String>>() {    // a DoFn as an anonymous inner class instance

                    private final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

                    @ProcessElement
                    public void processElement(@Element String[] fields, OutputReceiver<KV<String, String>> out) {
                        String guid = null;
                        String firstName = null;
                        String lastName = null;
                        String dob = null;
                        String postalCode = null;
                        for (String field : fields) {
                            LOG.info("field content: " + field.toString());
                            String[] fieldKeyValue = field.split(":");
                            if (fieldKeyValue.length == 2) {
                                String key = fieldKeyValue[0].trim().toLowerCase();
                                String value = fieldKeyValue[1].trim().toLowerCase();
                                if (key.equals("guid")) {
                                    guid = value;
                                    LOG.info("found guid: " + guid);
                                } else if (key.equals("firstname")) {
                                    firstName = value;
                                    LOG.info("found firstName: " + firstName);
                                } else if (key.equals("lastname")) {
                                    lastName = value;
                                    LOG.info("found lastName: " + lastName);
                                } else if (key.equals("dob")) {
                                    dob = value;
                                    LOG.info("found dob: " + dob);
                                } else if (key.equals("postalcode")) {
                                    postalCode = value;
                                    LOG.info("found postalCode: " + postalCode);
                                }
                            }
                        }


                        if (guid != null) {
                            out.output(KV.of("firstname:".concat(firstName), guid));
                            out.output(KV.of("lastname:".concat(lastName), guid));
                            out.output(KV.of("dob:".concat(dob), guid));
                            out.output(KV.of("postalcode:".concat(postalCode), guid));
                        }
                    }
                })).apply("Writing field indexes into redis",
                RedisIO.write().withMethod(RedisIO.Write.Method.SADD)
                        .withEndpoint(options.getRedisHost(), options.getRedisPort()));

        recordSet.apply(
                "Processing Payroll Provider ID",     // the transform name
                ParDo.of(new DoFn<String[], KV<String, KV<String, String>>>() {    // a DoFn as an anonymous inner class instance

                    private final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

                    @ProcessElement
                    public void processElement(@Element String[] fields,
                                     OutputReceiver<KV<String, KV<String, String>>> out) {
                        String guid = null;
                        String ppid = null;
                        for (String field : fields) {
                            LOG.info("field content: " + field.toString());
                            String[] fieldKeyValue = field.split(":");
                            if (fieldKeyValue.length == 2) {
                                String key = fieldKeyValue[0].trim().toLowerCase();
                                String value = fieldKeyValue[1].trim().toLowerCase();
                                if (key.equals("guid")) {
                                    guid = value;
                                    LOG.info("found guid: " + guid);
                                } else if (key.equals("pid")) {
                                    ppid = value;
                                    LOG.info("payroll provider: " + ppid);
                                }
                            }
                        }

                        if (guid != null && ppid != null) {
                            out.output(KV.of("hash11:".concat(guid), KV.of("hash12", ppid)));
                            LOG.info("Created the hash...");
                        }
                    }
                })).apply("Writing Hash Data into Redis",
                ParDo.of(new CustomRedisIODoFun(options.getRedisHost(), options.getRedisPort())));

        p.run();
    }
}
