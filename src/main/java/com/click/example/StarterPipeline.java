package com.click.example;

import com.click.example.functions.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
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
                "ReadLines", TextIO.read().from(ValueProvider.NestedValueProvider.of(options.getInputFile(),
                         (SerializableFunction<String, String>) file -> file)));
        /**
         * Count the number of records, Count.globally() must be used
         */
        PCollection<String> totalCount = lines.apply("Counting Total records", Count.globally())
                .apply("Logging Total Records", ParDo.of(new LogTotalRecords()));

        /**
         * Transform to get the name of filename
         */
        PCollection<String> filenameList =
                p.apply("Get the Filename", FileIO.match().filepattern(options.getInputFile()))
                        .apply(FileIO.readMatches())
                        .apply(MapElements.into(TypeDescriptors.strings())
                                .via((FileIO.ReadableFile file) -> {
                                    String f = file.getMetadata().resourceId().getFilename();
                                    LOGGER.info(" filename is: " + f);
                                    return f;
                                }));

        /**
         *  Pipeline for a transform that will count the number of success and failed records
         *  that produces two collections, i.e., validatedRecordCollection and invalidatedRecordCollection
         */

        final TupleTag<String> validRecords = new TupleTag<>(){};
        final TupleTag<String> invalidRecords = new TupleTag<>(){};

        PCollectionTuple mixedCollection =
                lines.apply(ParDo
                        .of(new DoFn<String, String>() {
                            private Boolean isNullOrEmpty(String value) {
                                return (value == null || value.isEmpty());
                            }
                            private Boolean isRecordValid(String recordLine) {
                                if (isNullOrEmpty(recordLine)) {
                                    return false;
                                }
                                String[] fields = recordLine.split(REGEX_LINE_SPLITTER_PIPE);
                                Boolean isRecordDirty = false;

                                if(fields != null && fields.length > 0) {
                                    for (String field : fields) {
                                        if(isNullOrEmpty(field)) {
                                            isRecordDirty = true;
                                        }
                                    }
                                }
                                return !isRecordDirty;
                            }

                            @ProcessElement
                            public void processElement(@Element String line, MultiOutputReceiver out) {
                                if(isRecordValid(line)) {
                                    out.get(validRecords).output(line);
                                } else {
                                    out.get(invalidRecords).output(line);
                                }
                            }
                        })
                        .withOutputTags(validRecords, TupleTagList.of(invalidRecords)));

        PCollection<String> validatedRecordCollection = mixedCollection.get(validRecords);

        PCollection<String> invalidatedRecordCollection = mixedCollection.get(invalidRecords);

        /**
         * Transform to count the number of valid records
         */
        PCollection<String> validCount = validatedRecordCollection.apply("Counting Valid Records", Count.globally())
                .apply("Logging Valid Records", ParDo.of(new LogValidRecords()));

        /**
         * Transform to count the number of invalid records
         */
        PCollection<String> invalidCount = invalidatedRecordCollection.apply("Counting Invalid Records", Count.globally())
                .apply("Logging Invalid Records", ParDo.of(new LogInvalidRecords()));

        PCollectionList<String> collectionList = PCollectionList.of(filenameList).and(validCount).and(invalidCount).and(totalCount);

        PCollection<String> mergedCollectionWithFlatten = collectionList
                .apply(Flatten.pCollections());

        mergedCollectionWithFlatten.apply("Custom Combine",
                Combine.globally(new CombiningTransforms())).apply(ParDo.of(new DoFn<String, Void>() {
            StringBuilder msg = new StringBuilder();
            @ProcessElement
            public void processElement(@Element String count) {
                if(count!=null && count.length() > 0) {
                    msg.append(count);
                    LOGGER.info("Processing completed: "+ msg);
                } else {
                    msg.append(count);
                    LOGGER.info("Processing failed.");
                }
            }
        }));

        PCollection<String[]> recordSet =
                lines.apply("Transform Record", ParDo.of(new TransformingData()));

        recordSet.apply("Processing Record", ParDo.of(new ProcessingRecords()))
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
