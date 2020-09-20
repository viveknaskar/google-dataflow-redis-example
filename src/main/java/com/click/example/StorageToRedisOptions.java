package com.click.example;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * StorageToRedisOptions is defined extending PipelineOptions with below properties
 */
public interface StorageToRedisOptions extends PipelineOptions {
    /**
     * Bucket where the text files are taken as input file
     */
    @Description("Path of the file to read from")
    @Default.String("DEFAULT")
    ValueProvider<String> getInputFile();
    void setInputFile(ValueProvider<String> value);

    /**
     * Memorystore/Redis instance host. Update with a running memorystore instance in the command-line to execute the pipeline
     */
    @Description("Redis host")
    @Default.String("DEFAULT")
    String getRedisHost();
    void setRedisHost(String value);

    /**
     * Memorystore/Redis instance port. The default port for Redis is 6379
     */
    @Description("Redis port")
    @Default.Integer(6379)
    Integer getRedisPort();
    void setRedisPort(Integer value);

}
