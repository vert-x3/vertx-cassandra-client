package io.vertx.cassandra;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class Hello {

    GenericContainer cassandraContainer = new GenericContainer("cassandra:3")
            .withExposedPorts(9046);

    @Before
    public void before() {
        cassandraContainer.start();
    }

    @After
    public void after() {
        cassandraContainer.stop();
    }

    @Test
    public void helloWorld() {
        System.out.println("it works");
    }
}
