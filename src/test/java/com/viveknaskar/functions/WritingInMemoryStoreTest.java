package com.viveknaskar.functions;

import com.github.fppt.jedismock.RedisServer;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class WritingInMemoryStoreTest {

    private static final String REDIS_HOST = "localhost";

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private static RedisServer server;
    private static int port;

    private static Jedis client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = RedisServer.newRedisServer(8000);
        server.start();
        port = server.getBindPort();
        client = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
    }

    @AfterClass
    public static void afterClass() {
        client.close();
        server.stop();
    }

    @Test
    public void testWriteHashWithConfig() {
        KV<String, String> fieldValue = KV.of("hash12", "p11");
        KV<String, KV<String, String>> record = KV.of("hash11:bbbbbb", fieldValue);

        PCollection<KV<String, KV<String, String>>> write = pipeline.apply(Create.of(record));

        write.apply("Writing Hash into Redis", WritingInMemoryStore.write()
                .withConnectionConfiguration(RedisConnectionConfiguration
                        .create(REDIS_HOST, port)));

        pipeline.run();

        Set<String> members = client.smembers("hash11:bbbbbb");
        boolean isMember = members.contains("hash12");
        Assert.assertTrue("The record should be a member in the set", isMember);
    }

}