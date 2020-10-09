package com.click.example;

import com.click.example.functions.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.click.example.constants.PipelineConstants.REGEX_LINE_SPLITTER_PIPE;

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
                "ReadLines", TextIO.read().from(options.getInputFile()));

        lines.apply("Counting Total records", Count.globally())
                .apply("Logging Total Records", ParDo.of(new LogTotalRecords()));

        final TupleTag<String> validRecords = new TupleTag<String>(){};
        final TupleTag<String> invalidRecords = new TupleTag<String>(){};

        PCollectionTuple mixedCollection =
                lines.apply(ParDo
                        .of(new SortRecords(validRecords, invalidRecords))
                        .withOutputTags(validRecords, TupleTagList.of(invalidRecords)));

        PCollection<String> validatedRecordCollection = mixedCollection.get(validRecords);

        PCollection<String> invalidatedRecordCollection = mixedCollection.get(invalidRecords);

        validatedRecordCollection.apply("Counting Valid Records", Count.globally())
                .apply("Logging Valid Records", ParDo.of(new LogValidRecords()));

        invalidatedRecordCollection.apply("Counting Invalid Records", Count.globally())
                .apply("Logging Invalid Records", ParDo.of(new LogInvalidRecords()));

        PCollection<String[]> recordSet =
                lines.apply("Transform Record", ParDo.of(new TransformingData()));

        PCollection<KV<String, String>> guidDataSet =
                recordSet.apply("Processing Record", ParDo.of(new ProcessingRecords()));

        guidDataSet.apply(
                "Creating GUID index",
                RedisIO.write().withMethod(RedisIO.Write.Method.SADD)
                        .withConnectionConfiguration(RedisConnectionConfiguration
                        .create(options.getRedisHost(), options.getRedisPort())));

        PCollection<KV<String, KV<String, String>>> ppidDataSet =
                recordSet.apply("Processing Payroll Provider ID", ParDo.of(new ProcessingPPID()));

        ppidDataSet.apply(
                "Creating PPID index",
                RedisHashIO.write().withConnectionConfiguration(RedisConnectionConfiguration
                        .create(options.getRedisHost(), options.getRedisPort())));

        p.run();
    }
}
