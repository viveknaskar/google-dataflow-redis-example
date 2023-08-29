package com.viveknaskar.functions;

import com.viveknaskar.StorageToRedisOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingRecordsTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static StorageToRedisOptions options = PipelineOptionsFactory.create()
            .as(StorageToRedisOptions.class);

    private static final String[] INPUT_DATA = new String[] {
            "xxxxxx","p11","tony","steve","stark","26071992","4444","male","9123456789"
    };

    @Test
    public void processElementForProcessingRecords() {

        MockitoAnnotations.initMocks(this);

        PCollection<String[]> input = pipeline.apply(Create.of(INPUT_DATA));

        PCollection<KV<String, String>> outputData = input
                .apply("Processing data", ParDo.of(new ProcessingRecords()));

        PAssert.that(outputData).containsInAnyOrder(
                KV.of("hash1:tony", "xxxxxx"),
                KV.of("hash2:steve", "xxxxxx"),
                KV.of("hash3:stark", "xxxxxx"),
                KV.of("hash4:26071992", "xxxxxx"),
                KV.of("hash5:4444", "xxxxxx"),
                KV.of("hash6:male", "xxxxxx"),
                KV.of("hash7:9123456789", "xxxxxx")
        );

        pipeline.run();

    }
}