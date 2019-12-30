/*
 * Copyright 2019 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.core.*;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.vertx.cassandra.CassandraClientOptions.DEFAULT_HOST;
import static java.util.stream.Collectors.toList;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@RunWith(VertxUnitRunner.class)
public abstract class CassandraClientTestBase {

  private static final int NATIVE_TRANSPORT_PORT = 9142;

  private final AtomicReference<Context> capturedContext = new AtomicReference<>();

  protected Vertx vertx;
  protected CQLDataLoader cqlDataLoader = new CQLDataLoader(EmbeddedCassandraServerHelper.getSession());
  protected CassandraClient client;

  @BeforeClass
  public static void startEmbeddedCassandra() throws Exception {
    String version = System.getProperty("java.version");
    // this statement can be removed only when this issue[https://github.com/jsevellec/cassandra-unit/issues/249] will be resolved
    if (!version.startsWith("1.8")) {
      throw new IllegalStateException("Only Java 8 is allowed for running tests. Your java version: " + version);
    }
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    client = CassandraClient.createNonShared(vertx, createClientOptions());
  }

  @After
  public void tearDown(TestContext testContext) {
    final Async async = testContext.async();
    client.close(testContext.asyncAssertSuccess(close -> async.countDown()));
    async.await();
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    vertx.close(testContext.asyncAssertSuccess());
  }

  protected CassandraClientOptions createClientOptions() {
    CassandraClientOptions cassandraClientOptions = new CassandraClientOptions();
    cassandraClientOptions.dataStaxClusterBuilder().withLocalDatacenter("datacenter1");
    return cassandraClientOptions.addContactPoint(InetSocketAddress.createUnresolved(DEFAULT_HOST, NATIVE_TRANSPORT_PORT));
  }

  protected void initializeRandomStringKeyspace(int rowsPerLetter) {
    cqlDataLoader.load(new RandomStringsDataSet(rowsPerLetter));
  }

  protected void initializeNamesKeyspace() {
    cqlDataLoader.load(new NamesDataSet());
  }

  protected void checkContext(TestContext testContext) {
    Context context = vertx.getOrCreateContext();
    if (capturedContext.compareAndSet(null, context)) {
      context.exceptionHandler(testContext::fail);
    } else {
      testContext.assertEquals(capturedContext.get(), context);
    }
  }

  protected static void getCassandraReleaseVersion(CassandraClient client, Handler<AsyncResult<String>> handler) {
    client.executeWithFullFetch("select release_version from system.local", ar -> {
      if (ar.succeeded()) {
        List<Row> result = ar.result();
        handler.handle(Future.succeededFuture(result.iterator().next().getString("release_version")));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private static class RandomStringsDataSet implements CQLDataSet {

    final int rowsPerLetter;

    RandomStringsDataSet(int rowsPerLetter) {
      this.rowsPerLetter = rowsPerLetter;
    }

    @Override
    public List<String> getCQLStatements() {
      Stream.Builder<String> builder = Stream.builder();
      builder.add("create table random_strings.random_string_by_first_letter (first_letter text, random_string text, primary key (first_letter, random_string));");
      for (char c = 'A'; c <= 'Z'; c++) {
        for (int i = 0; i < rowsPerLetter; i++) {
          String randomString = UUID.randomUUID().toString();
          String statement = String.format("INSERT INTO random_strings.random_string_by_first_letter (first_letter, random_string) VALUES ('%s', '%s')", c, c + randomString);
          builder.add(statement);
        }
      }
      return builder.build().collect(toList());
    }

    @Override
    public String getKeyspaceName() {
      return "random_strings";
    }

    @Override
    public boolean isKeyspaceCreation() {
      return true;
    }

    @Override
    public boolean isKeyspaceDeletion() {
      return true;
    }
  }

  private class NamesDataSet implements CQLDataSet {
    @Override
    public List<String> getCQLStatements() {
      return Collections.singletonList("create table names.names_by_first_letter (first_letter text, name text, primary key (first_letter, name));");
    }

    @Override
    public String getKeyspaceName() {
      return "names";
    }

    @Override
    public boolean isKeyspaceCreation() {
      return true;
    }

    @Override
    public boolean isKeyspaceDeletion() {
      return true;
    }
  }
}
