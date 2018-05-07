package io.vertx.cassandra;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class HelloTest {

  @Before
  public void before() throws InterruptedException, IOException, TTransportException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void helloWorld() {
    System.out.println("it works");
  }

  @Test
  public void helloWorld2() {
    System.out.println("it works");
  }
}
