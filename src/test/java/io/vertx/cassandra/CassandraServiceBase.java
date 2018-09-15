/*
 * Copyright 2018 The Vert.x Community.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.vertx.test.core.TestUtils.randomAlphaString;

/**
 * A base class, which should be used for in all test where Cassandra service is required.
 */
@RunWith(VertxUnitRunner.class)
public class CassandraServiceBase {

  private static final Logger log = LoggerFactory.getLogger(CassandraServiceBase.class);

  Vertx vertx = Vertx.vertx();

  static final String HOST = "localhost";
  static final int NATIVE_TRANSPORT_PORT = 9142;
  private static final int BATCH_INSERT_SIZE = 1_000;
  private static final int TIMES_TO_INSERT_BATCH = 100;

  private Context capturedContext = null;

  @Before
  public void before(TestContext context) throws IOException, TTransportException, InterruptedException {
    capturedContext = null;
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();

    Future<Void> namesKeyspaceInitialized = initializeNamesKeyspace();
    Future<Void> randomStringKeyspaceInitialized = initializeRandomStringKeyspace();

    Async async = context.async();
    CompositeFuture all = CompositeFuture.all(namesKeyspaceInitialized, randomStringKeyspaceInitialized);
    all.setHandler(h -> {
      if (h.failed()) {
        context.fail(h.cause());
      } else {
        async.countDown();
      }
    });
  }

  private Future<Void> initializeRandomStringKeyspace() {
    CassandraClient cassandraClient = client();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    return future.compose(connected -> {
      Future<ResultSet> createKeySpace = Future.future();
      cassandraClient.execute("CREATE KEYSPACE IF NOT EXISTS random_strings WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", createKeySpace);
      return createKeySpace;
    }).compose(keySpaceCreated -> {
      Future<ResultSet> createTable = Future.future();
      cassandraClient.execute("create table random_strings.random_string_by_first_letter (first_letter text, random_string text, primary key (first_letter, random_string));", createTable);
      return createTable;
    }).compose(tableCreated -> {
      Future<PreparedStatement> preparedStatement = Future.future();
      cassandraClient.prepare("INSERT INTO random_strings.random_string_by_first_letter (first_letter, random_string) VALUES (?, ?)", preparedStatement);
      return preparedStatement;
    }).compose(statementPrepared -> CompositeFuture.all(
      IntStream.range(1, TIMES_TO_INSERT_BATCH + 1).mapToObj(numb -> {
        BatchStatement statement = new BatchStatement();
        for (int i = 0; i < BATCH_INSERT_SIZE; i++) {
          String randomString = randomAlphaString(32);
          statement.add(statementPrepared.bind(randomString.substring(0, 1), randomString));
        }
        Future<ResultSet> executeQuery = Future.future();
        cassandraClient.execute(statement, executeQuery);
        return executeQuery;
      }).collect(Collectors.toList())
    )).compose(batchesExecuted -> {
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    });
  }

  private Future<Void> initializeNamesKeyspace() {
    CassandraClient cassandraClient = client();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    return future.compose(connected -> {
      Future<ResultSet> createKeySpace = Future.future();
      cassandraClient.execute("CREATE KEYSPACE IF NOT EXISTS names WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", createKeySpace);
      return createKeySpace;
    }).compose(keySpaceCreated -> {
      Future<ResultSet> createTable = Future.future();
      cassandraClient.execute("create table names.names_by_first_letter (first_letter text, name text, primary key (first_letter, name));", createTable);
      return createTable;
    }).compose(tableCreated -> {
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    });
  }

  private CassandraClient client() {
    return CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions()
        .addContactPoint(HOST)
        .setPort(NATIVE_TRANSPORT_PORT)
    );
  }

  public synchronized void checkContext(TestContext testContext) {
    if (capturedContext == null) {
      capturedContext = vertx.getOrCreateContext();
    } else if (!capturedContext.equals(vertx.getOrCreateContext())) {
      testContext.fail("context is not the same");
    }

    capturedContext.exceptionHandler(testContext::fail);
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}
