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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@RunWith(VertxUnitRunner.class)
public class StreamingTest extends CassandraClientTestBase {

  @Test
  public void testReadStream(TestContext testContext) {
    initializeRandomStringKeyspace(50);
    String query = "select random_string from random_strings.random_string_by_first_letter where first_letter = 'A'";
    Statement statement = SimpleStatement.newInstance(query)
      .setPageSize(5); // make sure data is not loaded at once from Cassandra
    Async async = testContext.async();
    client.queryStream(query, testContext.asyncAssertSuccess(stream -> {
      List<Row> items = Collections.synchronizedList(new ArrayList<>());
      AtomicInteger idx = new AtomicInteger();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        testContext.assertTrue(duration >= 5 * pause);
        async.countDown();
      }).exceptionHandler(testContext::fail).handler(item -> {
        items.add(item);
        int j = idx.getAndIncrement();
        if (j == 3 || j == 16 || j == 21 || j == 38 || j == 47) {
          stream.pause();
          int emitted = items.size();
          vertx.setTimer(pause, tid -> {
            testContext.assertTrue(emitted == items.size());
            stream.resume();
          });
        }
      });
    }));
  }

  @Test
  public void emptyStream(TestContext testContext) {
    initializeRandomStringKeyspace(1);
    String query = "select random_string from random_strings.random_string_by_first_letter where first_letter = '$'";
    Async async = testContext.async();
    client.queryStream(query, testContext.asyncAssertSuccess(stream -> {
      stream.endHandler(end -> async.countDown())
        .exceptionHandler(testContext::fail)
        .handler(item -> testContext.fail());
    }));
  }
}
