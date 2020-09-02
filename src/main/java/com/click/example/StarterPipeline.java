package com.click.example;

import com.click.example.functions.CustomRedisIODoFun;
import com.click.example.functions.ProcessingPPID;
import com.click.example.functions.ProcessingRecords;
import com.click.example.functions.TransformingData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

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
                            ParDo.of(new TransformingData()));

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
