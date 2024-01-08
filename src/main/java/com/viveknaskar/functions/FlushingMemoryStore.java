package com.viveknaskar.functions;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import javax.annotation.Nullable;
import java.util.Objects;

public class FlushingMemoryStore extends DoFn<Long, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlushingMemoryStore.class);

    public static FlushingMemoryStore.Read read() {
        return (new AutoValue_FlushingMemoryStore_Read.Builder())
                .setConnectionConfiguration(RedisConnectionConfiguration.create()).build();
    }

    @AutoValue
    public abstract static class Read extends PTransform<PCollection<Long>, PCollection<String>> {

        public Read() {
        }

        @Nullable
        abstract RedisConnectionConfiguration connectionConfiguration();

        @Nullable
        abstract Long expireTime();
        abstract FlushingMemoryStore.Read.Builder toBuilder();

        public FlushingMemoryStore.Read withEndpoint(String host, int port) {
            Preconditions.checkArgument(host != null, "host cannot be null");
            Preconditions.checkArgument(port > 0, "port cannot be negative or 0");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withHost(host).withPort(port)).build();
        }

        public FlushingMemoryStore.Read withAuth(String auth) {
            Preconditions.checkArgument(auth != null, "auth cannot be null");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withAuth(auth)).build();
        }

        public FlushingMemoryStore.Read withTimeout(int timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout cannot be negative");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withTimeout(timeout)).build();
        }

        public FlushingMemoryStore.Read withConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration) {
            Preconditions.checkArgument(connectionConfiguration != null, "connection cannot be null");
            return this.toBuilder().setConnectionConfiguration(connectionConfiguration).build();
        }

        public FlushingMemoryStore.Read withExpireTime(Long expireTimeMillis) {
            Preconditions.checkArgument(expireTimeMillis != null, "expireTimeMillis cannot be null");
            Preconditions.checkArgument(expireTimeMillis > 0L, "expireTimeMillis cannot be negative or 0");
            return this.toBuilder().setExpireTime(expireTimeMillis).build();
        }

        public PCollection<String> expand(PCollection<Long> input) {
            Preconditions.checkArgument(this.connectionConfiguration() != null, "withConnectionConfiguration() is required");
           return input.apply(ParDo.of(new FlushingMemoryStore.Read.ReadFn(this)));
        }

        @Setup
        public Jedis setup() {
            return Objects.requireNonNull(this.connectionConfiguration()).connect();
        }

        private static class ReadFn extends DoFn<Long, String> {
            private static final int DEFAULT_BATCH_SIZE = 1000;
            private final FlushingMemoryStore.Read spec;
            private transient Jedis jedis;
            private transient @Nullable Transaction transaction;
            private int batchCount;

            public ReadFn(FlushingMemoryStore.Read spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() {
                this.jedis = Objects.requireNonNull(this.spec.connectionConfiguration()).connect();
            }

            @StartBundle
            public void startBundle() {
                transaction = jedis.multi();
                batchCount = 0;
            }

            @ProcessElement
            public void processElement(@Element Long count, OutputReceiver<String> out) {
                batchCount++;

                if(count!=null && count > 0) {
                    jedis.flushDB();
                    LOGGER.info("*****The Memorystore is flushed*****");
                    out.output("SUCCESS");
                } else {
                    LOGGER.info("No Records are there in the input file");
                    out.output("FAILURE");
                }

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

            abstract FlushingMemoryStore.Read.Builder setExpireTime(Long expireTimeMillis);

            abstract FlushingMemoryStore.Read build();

            abstract FlushingMemoryStore.Read.Builder setConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration);

        }

    }

}