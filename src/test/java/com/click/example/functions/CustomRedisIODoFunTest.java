package com.click.example.functions;

import com.github.fppt.jedismock.RedisServer;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class CustomRedisIODoFunTest extends TestCase {

    private static RedisServer server = null;

    @Before
    public void before() throws IOException {
        server = RedisServer.newRedisServer();  // bind to a random port
        server.start();
    }

    @Test
    public void test() {
        Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
    }

    @After
    public void after() {
        server.stop();
        server = null;
    }

}