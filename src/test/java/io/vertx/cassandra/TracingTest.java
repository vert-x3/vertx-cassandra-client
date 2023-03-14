/*
 * Copyright 2021 Red Hat, Inc.
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

import io.vertx.cassandra.impl.tracing.QueryRequest;
import io.vertx.core.Context;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.junit.Assert.*;

public class TracingTest extends CassandraClientTestBase {

  VertxTracer tracer;

  @Override
  protected VertxOptions createVertxOptions() {
    TracingOptions tracingOptions = new TracingOptions()
      .setFactory(options -> new VertxTracer() {
        @Override
        public Object sendRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
          return tracer.sendRequest(context, kind, policy, request, operation, headers, tagExtractor);
        }

        @Override
        public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
          tracer.receiveResponse(context, response, payload, failure, tagExtractor);
        }
      });
    return super.createVertxOptions().setTracingOptions(tracingOptions);
  }

  @Override
  protected CassandraClientOptions createClientOptions() {
    return super.createClientOptions()
      .setTracingPolicy(TracingPolicy.ALWAYS);
  }

  @Test
  public void testSimpleStatement(TestContext ctx) {
    String cql = "select release_version from system.local";

    Context expectedContext = vertx.getOrCreateContext();

    Async async = ctx.async(3);
    tracer = new VertxTracer() {
      @Override
      public Object sendRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertEquals(TracingPolicy.ALWAYS, policy);
          QueryRequest queryRequest = (QueryRequest) request;
          assertEquals(cql, queryRequest.cql);
          async.countDown();
        });
        return null;
      }

      @Override
      public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertNull(failure);
          async.countDown();
        });
      }
    };

    client.executeWithFullFetch(cql).onComplete(ctx.asyncAssertSuccess(rows -> {
      async.countDown();
    }));
  }

  @Test
  public void testSimpleStatementFailure(TestContext ctx) {
    String cql = "select pone from fonky.family";

    Context expectedContext = vertx.getOrCreateContext();

    Async async = ctx.async(3);
    tracer = new VertxTracer() {
      @Override
      public Object sendRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertEquals(TracingPolicy.ALWAYS, policy);
          QueryRequest queryRequest = (QueryRequest) request;
          assertEquals(cql, queryRequest.cql);
          async.countDown();
        });
        return null;
      }

      @Override
      public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertNotNull(failure);
          async.countDown();
        });
      }
    };

    client.executeWithFullFetch(cql).onComplete(ctx.asyncAssertFailure(t -> async.countDown()));
  }

  @Test
  public void testBoundStatement(TestContext ctx) throws Exception {
    initializeRandomStringKeyspace();
    insertRandomStrings(5);
    String cql = "select random_string from random_strings.random_string_by_first_letter where first_letter = ?";

    Context expectedContext = vertx.getOrCreateContext();

    Async async = ctx.async(3);
    tracer = new VertxTracer() {
      @Override
      public Object sendRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertEquals(TracingPolicy.ALWAYS, policy);
          QueryRequest queryRequest = (QueryRequest) request;
          assertEquals(cql, queryRequest.cql);
          async.countDown();
        });
        return null;
      }

      @Override
      public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
        ctx.verify(unused -> {
          assertSame(expectedContext, context);
          assertNull(failure);
          async.countDown();
        });
      }
    };

    client.prepare(cql).onComplete(ctx.asyncAssertSuccess(ps -> {
      client.execute(ps.bind("B")).onComplete(ctx.asyncAssertSuccess(rs -> {
        async.countDown();
      }));
    }));

  }
}
