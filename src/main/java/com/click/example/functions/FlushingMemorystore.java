package com.click.example.functions;

import org.checkerframework.checker.nullness.qual.Nullable;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class FlushingMemorystore extends DoFn<Long, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlushingMemorystore.class);

    public static FlushingMemorystore.Read read() {
        return (new AutoValue_FlushingMemorystore_Read.Builder())
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
        abstract FlushingMemorystore.Read.Builder toBuilder();

        public FlushingMemorystore.Read withEndpoint(String host, int port) {
            Preconditions.checkArgument(host != null, "host cannot be null");
            Preconditions.checkArgument(port > 0, "port cannot be negative or 0");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withHost(host).withPort(port)).build();
        }

        public FlushingMemorystore.Read withAuth(String auth) {
            Preconditions.checkArgument(auth != null, "auth cannot be null");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withAuth(auth)).build();
        }

        public FlushingMemorystore.Read withTimeout(int timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout cannot be negative");
            return this.toBuilder().setConnectionConfiguration(this.connectionConfiguration().withTimeout(timeout)).build();
        }

        public FlushingMemorystore.Read withConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration) {
            Preconditions.checkArgument(connectionConfiguration != null, "connection cannot be null");
            return this.toBuilder().setConnectionConfiguration(connectionConfiguration).build();
        }

        public FlushingMemorystore.Read withExpireTime(Long expireTimeMillis) {
            Preconditions.checkArgument(expireTimeMillis != null, "expireTimeMillis cannot be null");
            Preconditions.checkArgument(expireTimeMillis > 0L, "expireTimeMillis cannot be negative or 0");
            return this.toBuilder().setExpireTime(expireTimeMillis).build();
        }

        public PCollection<String> expand(PCollection<Long> input) {
            Preconditions.checkArgument(this.connectionConfiguration() != null, "withConnectionConfiguration() is required");
           return input.apply(ParDo.of(new FlushingMemorystore.Read.ReadFn(this)));
        }

        @Setup
        public Jedis setup() {
            return this.connectionConfiguration().connect();
        }

        private static class ReadFn extends DoFn<Long, String> {
            private static final int DEFAULT_BATCH_SIZE = 1000;
            private final FlushingMemorystore.Read spec;
            private transient Jedis jedis;
            private transient Pipeline pipeline;
            private int batchCount;

            public ReadFn(FlushingMemorystore.Read spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() {
                this.jedis = this.spec.connectionConfiguration().connect();
            }

            @StartBundle
            public void startBundle() {
                this.pipeline = this.jedis.pipelined();
                this.pipeline.multi();
                this.batchCount = 0;
            }

            @ProcessElement
            public void processElement(@Element Long count, OutputReceiver<String> out) {
                batchCount++;

                if(count!=null && count > 0) {
                    if (pipeline.isInMulti()) {
                        pipeline.exec();
                        pipeline.sync();
                        jedis.flushDB();
                        LOGGER.info("*****The memorystore is flushed*****");
                    }
                    out.output("SUCCESS");
                } else {
                    LOGGER.info("No Records are there in the input file");
                    out.output("FAILURE");
                }

            }

            @FinishBundle
            public void finishBundle() {
                if (this.pipeline.isInMulti()) {
                    this.pipeline.exec();
                    this.pipeline.sync();
                }
                this.batchCount=0;
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

            abstract FlushingMemorystore.Read.Builder setExpireTime(Long expireTimeMillis);

            abstract FlushingMemorystore.Read build();

          abstract FlushingMemorystore.Read.Builder setConnectionConfiguration(RedisConnectionConfiguration connectionConfiguration);

        }

    }

}