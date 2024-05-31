/*
 * Copyright 2024 Red Hat, Inc.
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

import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.*;

public class StreamingTest extends CassandraClientTestBase {

  @Test
  public void testReadStream(TestContext testContext) throws Exception {
    initializeRandomStringKeyspace();
    insertRandomStrings(63);

    String query = "select random_string from random_strings.random_string_by_first_letter where first_letter = 'A'";
    SimpleStatement statement = SimpleStatement.newInstance(query)
      .setPageSize(5); // make sure data is not loaded at once from Cassandra

    Async async = testContext.async();

    List<Row> items = Collections.synchronizedList(new ArrayList<>());
    List<PagingState> pagingStates = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger idx = new AtomicInteger();

    client.queryStream(statement).onComplete(testContext.asyncAssertSuccess(stream -> {
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> testContext.verify(v -> {
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 5 * pause);
        for (int i = 1; i < pagingStates.size(); i++) {
          if (i >= 60) {
            assertNull(pagingStates.get(i));
          } else if (i % 5 == 0) {
            assertFalse(Arrays.equals(pagingStates.get(i).toBytes(), pagingStates.get(i - 1).toBytes()));
          } else {
            assertArrayEquals(pagingStates.get(i).toBytes(), pagingStates.get(i - 1).toBytes());
          }
        }
        async.countDown();
      })).exceptionHandler(testContext::fail).handler(item -> {
        items.add(item);
        int j = idx.getAndIncrement();
        pagingStates.add(stream.executionInfo().getSafePagingState());
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
  public void streamFetchesDoesNotOverflowDefault512KbJVMStack(TestContext testContext) throws Exception {
    int fetchSize = 100_000;
    initializeRandomStringKeyspace();
    insertRandomStrings(5_000);
    final SimpleStatement query = new SimpleStatementBuilder(
      String.format(
        "select random_string from random_strings.random_string_by_first_letter limit %d",
        fetchSize
      )
    ).setPageSize(fetchSize).build();
    Async async = testContext.async();
    client.queryStream(query, testContext.asyncAssertSuccess(stream -> {
      stream.endHandler(end -> async.countDown())
        .exceptionHandler(testContext::fail)
        .handler(item -> {});
    }));
  }

  @Test
  public void emptyStream(TestContext testContext) throws Exception {
    initializeRandomStringKeyspace();
    insertRandomStrings(1);
    String query = "select random_string from random_strings.random_string_by_first_letter where first_letter = '$'";
    Async async = testContext.async();
    client.queryStream(query).onComplete(testContext.asyncAssertSuccess(stream -> {
      testContext.assertNotNull(stream.columnDefinitions());
      stream.endHandler(end -> async.countDown())
        .exceptionHandler(testContext::fail)
        .handler(item -> testContext.fail());
    }));
  }

  @Test
  public void emptyStreamWithHandlerSetFirst(TestContext testContext) throws Exception {
    initializeRandomStringKeyspace();
    insertRandomStrings(1);
    String query = "select random_string from random_strings.random_string_by_first_letter where first_letter = '$'";
    Async async = testContext.async();
    client.queryStream(query, testContext.asyncAssertSuccess(stream -> {
        stream.handler(item -> testContext.fail())
          .endHandler(end -> async.countDown())
          .exceptionHandler(testContext::fail);
      })
    );
  }
}
