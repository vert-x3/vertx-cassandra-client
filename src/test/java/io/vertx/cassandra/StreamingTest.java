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

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

@RunWith(VertxUnitRunner.class)
public class StreamingTest extends CassandraServiceBase {

  @Test
  public void simpleStreamingReleaseVersionSelect(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.create(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async(2);
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      Future<CassandraRowStream> queryResult = Future.future();
      cassandraClient.queryStream("select release_version from system.local", queryResult);
      async.countDown();
      return queryResult;
    }).compose(stream -> {
      stream.handler(row -> {
        String release_version = row.getString("release_version");
        Assert.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
        async.countDown();
      });
      return Future.succeededFuture();
    }).setHandler(event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }

}
