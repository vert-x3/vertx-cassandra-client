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
package io.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.session.Session;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.cassandra.impl.tracing.QueryRequest;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.vertx.cassandra.impl.tracing.RequestTags.REQUEST_TAG_EXTRACTOR;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraClientImpl implements CassandraClient {

  static final String HOLDERS_LOCAL_MAP_NAME = "__vertx.cassandraClient.sessionHolders";

  final VertxInternal vertx;
  private final VertxTracer tracer;
  private final String clientName;
  private final CassandraClientOptions options;
  private final Map<String, SessionHolder> holders;
  private final ContextInternal creatingContext;

  private boolean closed;

  public CassandraClientImpl(Vertx vertx, String clientName, CassandraClientOptions options) {
    Objects.requireNonNull(vertx, "vertx");
    Objects.requireNonNull(clientName, "clientName");
    Objects.requireNonNull(options, "options");
    this.vertx = (VertxInternal) vertx;
    this.tracer = ((VertxInternal) vertx).tracer();
    this.clientName = clientName;
    this.options = options;
    this.creatingContext = ((VertxInternal) vertx).getOrCreateContext();
    holders = vertx.sharedData().getLocalMap(HOLDERS_LOCAL_MAP_NAME);
    SessionHolder current = holders.compute(clientName, (k, h) -> h == null ? new SessionHolder() : h.increment());
    creatingContext.addCloseHook(new Closeable() {
      @Override
      public void close(Promise<Void> completion) {
        CassandraClientImpl.this.close().onComplete(completion);
      }
    });
  }

  @Override
  public synchronized boolean isConnected() {
    if (closed) {
      return false;
    }
    Session s = holders.get(clientName).session;
    return s != null && !s.isClosed();
  }

  @Override
  public Future<List<Row>> executeWithFullFetch(String query) {
    return executeWithFullFetch(SimpleStatement.newInstance(query));
  }

  @Override
  public Future<List<Row>> executeWithFullFetch(Statement statement) {
    return execute(statement)
      .flatMap(ResultSet::all);
  }

  @Override
  public Future<ResultSet> execute(String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  @Override
  public <R> Future<R> execute(String query, Collector<Row, ?, R> collector) {
    return execute(SimpleStatement.newInstance(query), collector);
  }

  @Override
  public Future<ResultSet> execute(Statement statement) {
    return executeInternal(statement)
      .map(rs -> new ResultSetImpl(rs, vertx));
  }

  private Future<AsyncResultSet> executeInternal(Statement statement) {
    return getSession(vertx.getOrCreateContext())
      .flatMap(session -> {
        Object payload;
        if (tracer != null) {
          payload = sendRequest(session, statement);
        } else {
          payload = null;
        }
        Future<AsyncResultSet> future = Future.fromCompletionStage(session.executeAsync(statement), vertx.getContext());
        if (tracer != null) {
          future = future.onComplete(ar -> receiveResponse(payload, ar));
        }
        return future;
      });
  }

  private Object sendRequest(CqlSession session, Statement statement) {
    QueryRequest request = new QueryRequest(session, statement);
    return tracer.sendRequest(vertx.getContext(), SpanKind.RPC, options.getTracingPolicy(), request, "Query", (k, v) -> {
    }, REQUEST_TAG_EXTRACTOR);
  }

  private void receiveResponse(Object payload, AsyncResult<AsyncResultSet> asyncResult) {
    tracer.receiveResponse(vertx.getContext(), null, payload, asyncResult.cause(), TagExtractor.empty());
  }

  @Override
  public <R> Future<R> execute(Statement statement, Collector<Row, ?, R> collector) {
    return executeAndCollect(statement, collector);
  }

  private <C, R> Future<R> executeAndCollect(Statement statement, Collector<Row, C, R> collector) {
    C container = collector.supplier().get();
    BiConsumer<C, Row> accumulator = collector.accumulator();
    Function<C, R> finisher = collector.finisher();
    return queryStream(statement)
      .flatMap(cassandraRowStream -> {
        Promise<R> resultPromise = Promise.promise();
        cassandraRowStream.endHandler(end -> {
          R result = finisher.apply(container);
          resultPromise.complete(result);
        });
        cassandraRowStream.handler(row -> {
          accumulator.accept(container, row);
        });
        cassandraRowStream.exceptionHandler(resultPromise::fail);
        return resultPromise.future();
      });
  }

  @Override
  public Future<PreparedStatement> prepare(String query) {
    return getSession(vertx.getOrCreateContext())
      .flatMap(session -> Future.fromCompletionStage(session.prepareAsync(query), vertx.getContext()));
  }

  @Override
  public Future<PreparedStatement> prepare(SimpleStatement statement) {
    return getSession(vertx.getOrCreateContext())
      .flatMap(session -> Future.fromCompletionStage(session.prepareAsync(statement), vertx.getContext()));
  }

  @Override
  public Future<CassandraRowStream> queryStream(String sql) {
    return queryStream(SimpleStatement.newInstance(sql));
  }

  @Override
  public Future<CassandraRowStream> queryStream(Statement statement) {
    return executeInternal(statement)
      .map(rs -> {
        ResultSet resultSet = new ResultSetImpl(rs, vertx);
        return new CassandraRowStreamImpl(vertx.getContext(), resultSet);
      });
  }

  @Override
  public Future<Void> close() {
    ContextInternal context = vertx.getOrCreateContext();
    if (raiseCloseFlag()) {
      do {
        SessionHolder current = holders.get(clientName);
        SessionHolder next = current.decrement();
        if (next.refCount == 0) {
          if (holders.remove(clientName, current)) {
            if (current.session != null) {
              return Future.fromCompletionStage(current.session.closeAsync(), context);
            }
            break;
          }
        } else if (holders.replace(clientName, current, next)) {
          break;
        }
      } while (true);
    }
    return context.succeededFuture();
  }

  @Override
  public Future<Metadata> metadata() {
    return getSession(vertx.getOrCreateContext()).map(Session::getMetadata);
  }

  private synchronized boolean raiseCloseFlag() {
    if (!closed) {
      closed = true;
      return true;
    }
    return false;
  }

  synchronized Future<CqlSession> getSession(ContextInternal context) {
    if (closed) {
      return context.failedFuture("Client is closed");
    }
    SessionHolder holder = holders.get(clientName);
    if (holder.session != null) {
      return context.succeededFuture(holder.session);
    }
    return context.executeBlocking(this::connect, holder.connectionQueue);
  }

  private CqlSession connect() {
    SessionHolder current = holders.get(clientName);
    if (current == null) {
      throw new VertxException("Client closed while connecting", true);
    }
    if (current.session != null) {
      return current.session;
    }
    CqlSessionBuilder builder = options.dataStaxClusterBuilder();
    CqlSession session = builder.build();
    current = holders.compute(clientName, (k, h) -> h == null ? null : h.connected(session));
    if (current != null) {
      return current.session;
    } else {
      try {
        session.close();
      } catch (Exception ignored) {
      }
      throw new VertxException("Client closed while connecting", true);
    }
  }
}
