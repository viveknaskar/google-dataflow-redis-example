package com.viveknaskar.functions;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class WritingInMemoryStore {

    public static WritingInMemoryStore.Write write() {

        return (new AutoValue_WritingInMemoryStore_Write.Builder())
                .setConnectionConfiguration(RedisConnectionConfiguration.create()).build();

    }

    @AutoValue
    public abstract static class Write extends PTransform<PCollection<KV<String, KV<String, String>>>, PDone> {

        public Write() {
        }

        @Nullable
        abstract RedisConnectionConfiguration connectionConfiguration();

        @Nullable
        abstract Long expireTime();

        abstract WritingInMemoryStore.Write.Builder toBuilder();

        public WritingInMemoryStore.Write withEndpoint(String host, int port) {
            Preconditions.checkArgument(host != null, "host cannot be null");
            Preconditions.checkArgument(port > 0, "port cannot be negative or 0");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withHost(host).withPort(port)).build();
        }

        public WritingInMemoryStore.Write withAuth(String auth) {
            Preconditions.checkArgument(auth != null, "auth cannot be null");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withAuth(auth)).build();
        }

        public WritingInMemoryStore.Write withTimeout(int timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout cannot be negative");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withTimeout(timeout)).build();
        }

        public WritingInMemoryStore.Write withConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration) {
            Preconditions.checkArgument(connectionConfiguration != null, "connection cannot be null");
            return this.toBuilder().setConnectionConfiguration(connectionConfiguration).build();
        }

        public WritingInMemoryStore.Write withExpireTime(Long expireTimeMillis) {
            Preconditions.checkArgument(expireTimeMillis != null, "expireTimeMillis cannot be null");
            Preconditions.checkArgument(expireTimeMillis > 0L, "expireTimeMillis cannot be negative or 0");
            return this.toBuilder().setExpireTime(expireTimeMillis).build();
        }

        public PDone expand(PCollection<KV<String, KV<String, String>>> input) {
            Preconditions.checkArgument(this.connectionConfiguration() != null, "withConnectionConfiguration() is required");
            input.apply(ParDo.of(new WritingInMemoryStore.Write.WriteFn(this)));
            return PDone.in(input.getPipeline());
        }

        private static class WriteFn extends DoFn<KV<String, KV<String, String>>, Void> {
            private static final int DEFAULT_BATCH_SIZE = 1000;
            private final WritingInMemoryStore.Write spec;
            private transient Jedis jedis;
            private transient @Nullable Transaction transaction;
            private int batchCount;

            public WriteFn(WritingInMemoryStore.Write spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() {
                this.jedis = this.spec.connectionConfiguration().connect();
            }

            @StartBundle
            public void startBundle() {
                transaction = jedis.multi();
                batchCount = 0;
            }

            @ProcessElement
            public void processElement(DoFn<KV<String, KV<String, String>>, Void>.ProcessContext c) {
                KV<String, KV<String, String>> record = c.element();

                writeRecord(record);

                batchCount++;

                if (batchCount >= DEFAULT_BATCH_SIZE) {
                    transaction.exec();
                    transaction.multi();
                    batchCount = 0;
                }
            }

            private void writeRecord(KV<String, KV<String, String>> record) {
                String hashKey = record.getKey();
                KV<String, String> hashValue = record.getValue();
                String fieldKey = hashValue.getKey();
                String value = hashValue.getValue();

                transaction.sadd(hashKey, fieldKey, value);

            }

            @FinishBundle
            public void finishBundle() {
                if (batchCount > 0) {
                    transaction.exec();
                }
                if (transaction != null) {
                    transaction.close();
                }
                transaction = null;
                batchCount = 0;
            }

            @Teardown
            public void teardown() {
                this.jedis.close();
            }

        }

        @AutoValue.Builder
        abstract static class Builder {

            Builder() {
            }

            abstract WritingInMemoryStore.Write.Builder setExpireTime(Long expireTimeMillis);

            abstract WritingInMemoryStore.Write build();

            abstract WritingInMemoryStore.Write.Builder setConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration);

        }

    }

}