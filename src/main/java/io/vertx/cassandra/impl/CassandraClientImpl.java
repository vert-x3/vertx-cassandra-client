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

import com.datastax.driver.core.*;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.vertx.cassandra.impl.Util.handleOnContext;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraClientImpl implements CassandraClient {

  static final String HOLDERS_LOCAL_MAP_NAME = "__vertx.cassandraClient.sessionHolders";

  final VertxInternal vertx;
  private final String clientName;
  private final CassandraClientOptions options;
  private final Map<String, SessionHolder> holders;

  private boolean closed;

  public CassandraClientImpl(Vertx vertx, String clientName, CassandraClientOptions options) {
    Objects.requireNonNull(vertx, "vertx");
    Objects.requireNonNull(clientName, "clientName");
    Objects.requireNonNull(options, "options");
    this.vertx = (VertxInternal) vertx;
    this.clientName = clientName;
    this.options = options;
    holders = vertx.sharedData().getLocalMap(HOLDERS_LOCAL_MAP_NAME);
    SessionHolder current = holders.compute(clientName, (k, h) -> h == null ? new SessionHolder() : h.increment());
    Context context = Vertx.currentContext();
    if (context != null && context.owner() == vertx) {
      context.addCloseHook(this::close);
    }
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
  public CassandraClient executeWithFullFetch(String query, Handler<AsyncResult<List<Row>>> resultHandler) {
    return executeWithFullFetch(new SimpleStatement(query), resultHandler);
  }

  @Override
  public CassandraClient executeWithFullFetch(Statement statement, Handler<AsyncResult<List<Row>>> resultHandler) {
    execute(statement, exec -> {
      if (exec.succeeded()) {
        ResultSet resultSet = exec.result();
        resultSet.all(resultHandler);
      } else {
        resultHandler.handle(Future.failedFuture(exec.cause()));
      }
    });
    return this;
  }

  public CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler) {
    return execute(new SimpleStatement(query), resultHandler);
  }

  @Override
  public <R> CassandraClient execute(String query, Collector<Row, ?, R> collector, Handler<AsyncResult<R>> asyncResultHandler) {
    return execute(new SimpleStatement(query), collector, asyncResultHandler);
  }

  @Override
  public CassandraClient execute(Statement statement, Handler<AsyncResult<ResultSet>> resultHandler) {
    ContextInternal context = vertx.getOrCreateContext();
    getSession(context, sess -> {
      if (sess.succeeded()) {
        handleOnContext(sess.result().executeAsync(statement), context, rs -> new ResultSetImpl(rs, vertx), resultHandler);
      } else {
        resultHandler.handle(Future.failedFuture(sess.cause()));
      }
    });
    return this;
  }

  @Override
  public <R> CassandraClient execute(Statement statement, Collector<Row, ?, R> collector, Handler<AsyncResult<R>> asyncResultHandler) {
    executeAndCollect(statement, collector, asyncResultHandler);
    return this;
  }

  private <C, R> void executeAndCollect(Statement statement, Collector<Row, C, R> collector, Handler<AsyncResult<R>> asyncResultHandler) {
    Promise<CassandraRowStream> cassandraRowStreamPromise = Promise.promise();
    queryStream(statement, cassandraRowStreamPromise);
    C container = collector.supplier().get();
    BiConsumer<C, Row> accumulator = collector.accumulator();
    Function<C, R> finisher = collector.finisher();
    cassandraRowStreamPromise.future().compose(cassandraRowStream -> {
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
    }).setHandler(asyncResultHandler);
  }

  @Override
  public CassandraClient prepare(String query, Handler<AsyncResult<PreparedStatement>> resultHandler) {
    ContextInternal context = vertx.getOrCreateContext();
    getSession(context, sess -> {
      if (sess.succeeded()) {
        handleOnContext(sess.result().prepareAsync(query), context, resultHandler);
      } else {
        resultHandler.handle(Future.failedFuture(sess.cause()));
      }
    });
    return this;
  }

  @Override
  public CassandraClient queryStream(String sql, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    return queryStream(new SimpleStatement(sql), rowStreamHandler);
  }

  @Override
  public CassandraClient queryStream(Statement statement, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    ContextInternal context = vertx.getOrCreateContext();
    getSession(context, sess -> {
      if (sess.succeeded()) {
        handleOnContext(sess.result().executeAsync(statement), context, rs -> {
          ResultSet resultSet = new ResultSetImpl(rs, vertx);
          return new CassandraRowStreamImpl(context, resultSet);
        }, rowStreamHandler);
      } else {
        rowStreamHandler.handle(Future.failedFuture(sess.cause()));
      }
    });
    return this;
  }

  @Override
  public CassandraClient close() {
    return close(null);
  }

  @Override
  public CassandraClient close(Handler<AsyncResult<Void>> closeHandler) {
    if (raiseCloseFlag()) {
      do {
        SessionHolder current = holders.get(clientName);
        SessionHolder next = current.decrement();
        if (next.refCount == 0) {
          if (holders.remove(clientName, current)) {
            if (current.session != null) {
              handleOnContext(current.session.closeAsync(), vertx.getOrCreateContext(), closeHandler);
              return this;
            }
            break;
          }
        } else if (holders.replace(clientName, current, next)) {
          break;
        }
      } while (true);
    }
    if (closeHandler != null) {
      closeHandler.handle(Future.succeededFuture());
    }
    return this;
  }

  private synchronized boolean raiseCloseFlag() {
    if (!closed) {
      closed = true;
      return true;
    }
    return false;
  }

  synchronized void getSession(ContextInternal context, Handler<AsyncResult<Session>> handler) {
    if (closed) {
      handler.handle(Future.failedFuture("Client is closed"));
    } else {
      SessionHolder holder = holders.get(clientName);
      if (holder.session != null) {
        handler.handle(Future.succeededFuture(holder.session));
      } else {
        context.executeBlocking(promise -> {
          connect(promise);
        }, holder.connectionQueue, handler);
      }
    }
  }

  private void connect(Promise<Session> promise) {
    SessionHolder current = holders.get(clientName);
    if (current == null) {
      promise.fail("Client closed while connecting");
      return;
    }
    if (current.session != null) {
      promise.complete(current.session);
      return;
    }
    Cluster.Builder builder = options.dataStaxClusterBuilder();
    if (builder.getContactPoints().isEmpty()) {
      builder.addContactPoint(CassandraClientOptions.DEFAULT_HOST);
    }
    Cluster cluster = builder.build();
    Session session = cluster.connect(options.getKeyspace());
    current = holders.compute(clientName, (k, h) -> h == null ? null : h.connected(session));
    if (current != null) {
      promise.complete(current.session);
    } else {
      try {
        session.close();
      } catch (Exception ignored) {
      }
      promise.fail("Client closed while connecting");
    }
  }
}
