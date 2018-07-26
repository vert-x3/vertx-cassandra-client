package io.vertx.cassandra;

import com.datastax.driver.core.Row;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.regex.Pattern;

@RunWith(VertxUnitRunner.class)
public class SharedTest extends CassandraServiceBase {

  @Test
  public void executeQueryFormLookUp(TestContext context) {
    CassandraClient cassandraClientInit = CassandraClient.createShared(vertx, new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT));
    cassandraClientInit.connect();
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClientInit.connect(future);
    future.compose(connected -> {
      CassandraClient clientFormLookUp = CassandraClient.createShared(vertx);
      Future<List<Row>> queryResult = Future.future();
      clientFormLookUp.executeWithFullFetch("select release_version from system.local", queryResult);
      return queryResult;
    }).compose(resultSet -> {
      CassandraClient clientFormLookUp = CassandraClient.createShared(vertx);
      String release_version = resultSet.iterator().next().getString("release_version");
      Assert.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
      Future<Void> disconnectFuture = Future.future();
      clientFormLookUp.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.countDown();
    });
  }

}
