package com.viveknaskar.functions;

import com.viveknaskar.StorageToRedisOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransformingDataTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static final StorageToRedisOptions options = PipelineOptionsFactory.create()
            .as(StorageToRedisOptions.class);

    private static final String INPUT_DATA =
            "xxxxxx|p11|tony|steve|stark|26071992|4444|male|91234567789";

    private static final String[] OUTPUT_DATA = new String[] {
            "xxxxxx","p11","tony","steve","stark","26071992","4444","male","91234567789"
    };

    @Test
    public void processElementForTransformingData() {

        MockitoAnnotations.initMocks(this);

        PCollection<String> input = pipeline.apply(Create.of(INPUT_DATA));

        PCollection<String[]> outputData = input
                .apply("Processing data", ParDo.of(new TransformingData()));

        PAssert.that(outputData).containsInAnyOrder(OUTPUT_DATA);

        pipeline.run();

    }
}