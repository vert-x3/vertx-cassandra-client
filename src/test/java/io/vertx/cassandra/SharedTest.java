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
      checkContext(context);
      CassandraClient clientFormLookUp = CassandraClient.createShared(vertx);
      Future<List<Row>> queryResult = Future.future();
      clientFormLookUp.executeWithFullFetch("select release_version from system.local", queryResult);
      return queryResult;
    }).compose(resultSet -> {
      checkContext(context);
      CassandraClient clientFormLookUp = CassandraClient.createShared(vertx);
      String release_version = resultSet.iterator().next().getString("release_version");
      Assert.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
      Future<Void> disconnectFuture = Future.future();
      clientFormLookUp.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(event -> {
      checkContext(context);
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.countDown();
    });
  }

}
