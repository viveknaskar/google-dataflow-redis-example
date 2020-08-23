package com.click.example;

import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class CustomRedisIODoFun extends DoFn<KV<String, KV<String, String>>, Void> {

    private final Logger LOGGER = LoggerFactory.getLogger(StarterPipeline.class);

    private static final int DEFAULT_BATCH_SIZE = 100;
    private final String host;
    private final int port;
    private final int timeout;
    private transient Jedis jedis;
    private transient Pipeline pipeline;
    private int batchCount;

    public CustomRedisIODoFun(String host, int port) {
        this(host, port, 18000);
    }

    public CustomRedisIODoFun(String host, int port, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    @Setup
    public void setup() {
        jedis = RedisConnectionConfiguration.create().withHost(host).withPort(port).withTimeout(timeout).connect();
        LOGGER.debug("Redis Connected successfully...");
    }

    @StartBundle
    public void startBundle() {
        pipeline = jedis.pipelined();
        pipeline.multi();
        batchCount = 0;
        LOGGER.debug("Redis Pipeline configured...");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<String, KV<String, String>> record = c.element();

        writeRecord(record);

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
            pipeline.exec();
            pipeline.sync();
            pipeline.multi();
            batchCount = 0;
            LOGGER.debug("Batch Write Complete and pipeline flushed!");
        }
        LOGGER.debug("Record Processed!");
    }

    private void writeRecord(KV<String, KV<String, String>> record) {
        String hashKey = record.getKey();
        KV<String, String> hashValue = record.getValue();
        String fieldKey = hashValue.getKey();
        String value = hashValue.getValue();

        pipeline.hset(hashKey, fieldKey, value);

    }

    @FinishBundle
    public void finishBundle() {
        if (pipeline.isInMulti()) {
            pipeline.exec();
            pipeline.sync();
        }
        batchCount = 0;
        LOGGER.debug("Pipeline Flushed!");
    }

    @Teardown
    public void teardown() {
        jedis.close();
        LOGGER.debug("Connection Closed!");
    }
}
