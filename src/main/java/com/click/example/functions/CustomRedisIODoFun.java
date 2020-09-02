package com.click.example.functions;

import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class CustomRedisIODoFun extends DoFn<KV<String, KV<String, String>>, Void> {

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
    }

    @StartBundle
    public void startBundle() {
        pipeline = jedis.pipelined();
        pipeline.multi();
        batchCount = 0;
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
        }
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
    }

    @Teardown
    public void teardown() {
        jedis.close();
    }
}
