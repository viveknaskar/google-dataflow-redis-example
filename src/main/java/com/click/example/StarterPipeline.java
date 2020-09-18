package com.click.example;

import com.click.example.functions.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);


    public static void main(String[] args) {
        /**
         * Constructed StorageToRedisOptions object using the method PipelineOptionsFactory.fromArgs
         * to read options from command-line
         */
        StorageToRedisOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(StorageToRedisOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply(
                "ReadLines", TextIO.read().from(options.getInputFile())
        );

        lines.apply(Count.globally()).apply("Count the Total Records", ParDo.of(new CountTotalRecords()));

        PCollection<String[]> recordSet =
                lines.apply("Transform Record", ParDo.of(new TransformingData()));

        recordSet.apply(
                        "Processing Record", ParDo.of(new ProcessingRecords()))
                        .apply("Writing field indexes into redis",
                        RedisIO.write().withMethod(RedisIO.Write.Method.SADD)
                                .withEndpoint(options.getRedisHost(), options.getRedisPort()));

        recordSet.apply(
                "Processing Payroll Provider ID",
                ParDo.of(new ProcessingPPID())).apply("Writing Hash Data into Redis",
                ParDo.of(new CustomRedisIODoFun(options.getRedisHost(), options.getRedisPort())));
        p.run();
    }
}
