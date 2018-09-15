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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@RunWith(VertxUnitRunner.class)
public class StreamingTest extends CassandraServiceBase {

  @Test
  public void testReadStream(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      checkContext(context);
      Future<CassandraRowStream> queryResult = Future.future();
      cassandraClient.queryStream("select random_string from random_strings.random_string_by_first_letter where first_letter = 'A'", queryResult);
      return queryResult;
    }).compose(stream -> {
      checkContext(context);
      List<Row> items = new ArrayList<>();
      AtomicInteger idx = new AtomicInteger();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        checkContext(context);
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        context.assertTrue(duration >= 3 * pause);
        cassandraClient.disconnect(disconnected -> {
          checkContext(context);
          async.countDown();
        });
      }).exceptionHandler(context::fail)
        .handler(item -> {
          checkContext(context);
          items.add(item);
          int j = idx.getAndIncrement();
          if (j == 3 || j == 16 || j == 38) {
            stream.pause();
            int emitted = items.size();
            vertx.setTimer(pause, tid -> {
              context.assertTrue(emitted == items.size());
              stream.resume();
            });
          }
        });

      return Future.succeededFuture();
    }).setHandler(h -> {
      checkContext(context);
      if (h.failed()) {
        context.fail(h.cause());
      }
    });
  }

  @Test
  public void emptyStream(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );
    Async async = context.async();
    Future<Void> future = Future.future();
    cassandraClient.connect(future);
    future.compose(connected -> {
      checkContext(context);
      Future<CassandraRowStream> queryResult = Future.future();
      cassandraClient.queryStream("select random_string from random_strings.random_string_by_first_letter where first_letter = 'I WANT EMPTY RESULT'", queryResult);
      return queryResult;
    }).compose(stream -> {
      stream.endHandler(end -> cassandraClient.disconnect(disconnected -> {
        checkContext(context);
        async.countDown();
      }))
        .exceptionHandler(context::fail)
        .handler(item -> context.fail());
      return Future.succeededFuture();
    }).setHandler(h -> {
      checkContext(context);
      if (h.failed()) {
        context.fail(h.cause());
      }
    });
  }

}
