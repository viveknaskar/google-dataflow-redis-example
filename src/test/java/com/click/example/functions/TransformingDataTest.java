package com.click.example.functions;

import com.click.example.StorageToRedisOptions;
import junit.framework.TestCase;
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
public class TransformingDataTest extends TestCase {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static StorageToRedisOptions options = PipelineOptionsFactory.create()
            .as(StorageToRedisOptions.class);

    private static final String INPUT_DATA =
            "p11|xxxxxx|tony|steve|stark|26071992|4444|male";

    private static final String[] OUTPUT_DATA = new String[] {
            "p11","xxxxxx","tony","steve","stark","26071992","4444","male"
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