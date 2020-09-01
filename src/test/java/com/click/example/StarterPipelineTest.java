package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StarterPipelineTest {

    @InjectMocks
    private StarterPipeline starterPipeline;

    static StorageToRedisOptions options = PipelineOptionsFactory.create()
            .as(StorageToRedisOptions.class);

    Pipeline pipeline = TestPipeline.create(options);

    /**
     * Update the project and a running redis instance
     */
    @Test
    public void testMain() {
        String args[] = {"--project=your-project-id",
                "--jobName=dataflow-test-redis-job",
                "--redisHost=127.0.0.1",
                "--inputFile=gs://cloud-dataflow-bucket-input/*.txt",
                "--stagingLocation=gs://cloud-dataflow-pipeline-bucket/staging/",
                "--dataflowJobFile=gs://cloud-dataflow-pipeline-bucket/templates/dataflow-custom-redis-template",
                "--gcpTempLocation=gs://cloud-dataflow-pipeline-bucket/tmp/",
                "--runner=DataflowRunner"
        };

        StarterPipeline.main(args);
        Assert.assertNotNull(args);

    }
}