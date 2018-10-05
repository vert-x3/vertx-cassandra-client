package io.vertx.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * This test suit is dedicated to check that all handler are called from the same context.
 */
@RunWith(VertxUnitRunner.class)
public class ThreadingCheckTest extends CassandraServiceBase {

  private static String NAME = "Pavel";

  @Test
  public void checkStreamHandlers(TestContext testContext) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = testContext.async(1);
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      checkContext(testContext);
      Future<CassandraRowStream> queryResult = Future.future();
      cassandraClient.queryStream("select random_string from random_strings.random_string_by_first_letter where first_letter = 'A'", queryResult);
      return queryResult;
    }).compose(stream -> {
      checkContext(testContext);
      stream.endHandler(end -> {
        checkContext(testContext);
        async.countDown();
      })
        .exceptionHandler(throwable -> checkContext(testContext))
        .handler(item -> checkContext(testContext));
      return Future.succeededFuture();
    }).setHandler(h -> {
      checkContext(testContext);
      if (h.failed()) {
        testContext.fail(h.cause());
      }
    });
  }

  @Test
  public void checkConnectDisconnectPrepareAndQueryHandlers(TestContext testContext) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = testContext.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      checkContext(testContext);
      Future<PreparedStatement> queryResult = Future.future();
      cassandraClient.prepare("INSERT INTO names.names_by_first_letter (first_letter, name) VALUES (?, ?)", queryResult);
      return queryResult;
    }).compose(prepared -> {
      checkContext(testContext);
      Future<ResultSet> executionQuery = Future.future();
      Statement query = prepared.bind("P", "Pavel");
      cassandraClient.execute(query, executionQuery);
      return executionQuery;
    }).compose(executed -> {
      checkContext(testContext);
      Future<List<Row>> executionQuery = Future.future();
      cassandraClient.executeWithFullFetch("select NAME as n from names.names_by_first_letter where first_letter = 'P'", executionQuery);
      return executionQuery;
    }).compose(executed -> {
      checkContext(testContext);
      testContext.assertTrue(executed.get(0).getString("n").equals(NAME));
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      checkContext(testContext);
      if (event.failed()) {
        testContext.fail(event.cause());
      } else {
        async.countDown();
      }
    });
  }
}
