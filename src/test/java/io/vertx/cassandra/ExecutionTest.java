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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Test Casssandra client on how it executes queries.
 */
@RunWith(VertxUnitRunner.class)
public class ExecutionTest extends CassandraServiceBase {

  private static final Logger log = LoggerFactory.getLogger(ExecutionTest.class);
  private static String NAME = "Pavel";

  @Test
  public void tableHaveSomeRows(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      checkContext(context);
      Future<ResultSet> queryResult = Future.future();
      cassandraClient.execute("select count(*) as cnt from random_strings.random_string_by_first_letter", queryResult);
      return queryResult;
    }).compose((ResultSet resultSet) -> {
      checkContext(context);
      Future<Row> oneRowFuture = Future.future();
      resultSet.one(oneRowFuture);
      return oneRowFuture;
    }).compose(one -> {
      checkContext(context);
      context.assertTrue(one.getLong("cnt") > 0);
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      checkContext(context);
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.countDown();
    });
  }

  @Test
  public void simpleExecuteWithBigAmountOfFetches(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      Future<List<Row>> queryResult = Future.future();
      SimpleStatement statement = new SimpleStatement("select random_string from random_strings.random_string_by_first_letter where first_letter = 'B'");
      // we would like to test that we are able to handle a lot of fetches.
      // that is why we are setting fetch size here to 1
      statement.setFetchSize(1);
      cassandraClient.executeWithFullFetch(statement, queryResult);
      return queryResult;
    }).compose((List<Row> resultSet) -> {
      for (Row row: resultSet) {
        String selectedString = row.getString(0);
        context.assertNotNull(selectedString);
      }
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.countDown();
    });
  }

  @Test
  public void simpleReleaseVersionSelect(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      Future<List<Row>> queryResult = Future.future();
      cassandraClient.executeWithFullFetch("select release_version from system.local", queryResult);
      return queryResult;
    }).compose(resultSet -> {
      String release_version = resultSet.iterator().next().getString("release_version");
      Assert.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.countDown();
    });
  }

  @Test
  public void preparedStatementsShouldWork(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      Future<PreparedStatement> queryResult = Future.future();
      cassandraClient.prepare("INSERT INTO names.names_by_first_letter (first_letter, name) VALUES (?, ?)", queryResult);
      return queryResult;
    }).compose(prepared -> {
      Future<ResultSet> executionQuery = Future.future();
      Statement query = prepared.bind("P", "Pavel");
      cassandraClient.execute(query, executionQuery);
      return executionQuery;
    }).compose(executed -> {
      Future<List<Row>> executionQuery = Future.future();
      cassandraClient.executeWithFullFetch("select NAME as n from names.names_by_first_letter where first_letter = 'P'", executionQuery);
      return executionQuery;
    }).compose(executed -> {
      context.assertTrue(executed.get(0).getString("n").equals(NAME));
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      if (event.failed()) {
        context.fail(event.cause());
      } else {
        async.countDown();
      }
    });
  }
}
