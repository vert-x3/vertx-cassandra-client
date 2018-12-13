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
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.*;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;
import io.vertx.cassandra.VertxMappingManager;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.vertx.cassandra.impl.Util.handleOnContext;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraClientImpl implements CassandraClient {

  private static final String DS_LOCAL_MAP_NAME = "__vertx.CassandraClient.datasources";

  private final Vertx vertx;
  private final CassandraHolder cassandraHolder;

  public CassandraClientImpl(Vertx vertx, String dataSourceName, CassandraClientOptions cassandraClientOptions) {
    this.vertx = vertx;
    cassandraHolder = lookupHolder(dataSourceName, cassandraClientOptions);
    Context ctx = Vertx.currentContext();
    if (ctx != null && ctx.owner() == vertx) {
      ctx.addCloseHook(v -> cassandraHolder.close());
    }
  }

  class CassandraHolder implements Closeable, Shareable {
    int refCount = 1;
    AtomicReference<Session> session = new AtomicReference<>(null);
    CassandraClientOptions options;
    Runnable closeRunner;

    public CassandraHolder(CassandraClientOptions options, Runnable closeRunner) {
      this.options = options;
      this.closeRunner = closeRunner;
    }

    synchronized void incRefCount() {
      refCount++;
    }

    @Override
    public void close() {
      synchronized (this) {
        --refCount;
        if (refCount == 0 && session.get() != null) {
          session.get().close();
        }
      }

      if (refCount == 0 && closeRunner != null) {
        closeRunner.run();
      }
    }
  }

  private LocalMap<String, CassandraHolder> cassandraHolderLocalMap() {
    return vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
  }

  private CassandraHolder lookupHolder(String dataSourceName, CassandraClientOptions cassandraClientOptions) {
    LocalMap<String, CassandraHolder> map = cassandraHolderLocalMap();
    synchronized (map) {
      CassandraHolder theHolder = map.get(dataSourceName);
      if (theHolder == null) {
        theHolder = new CassandraHolder(cassandraClientOptions, () -> removeFromMap(map, dataSourceName));
        map.put(dataSourceName, theHolder);
      } else {
        theHolder.incRefCount();
      }
      return theHolder;
    }
  }

  private void removeFromMap(LocalMap<String, CassandraHolder> map, String dataSourceName) {
    synchronized (map) {
      map.remove(dataSourceName);
      if (map.isEmpty()) {
        map.close();
      }
    }
  }

  @Override
  public CassandraClient connect() {
    return connect(null);
  }

  @Override
  public boolean isConnected() {
    Session session = this.cassandraHolder.session.get();
    if (session == null) {
      return false;
    } else {
      return !session.isClosed();
    }
  }

  @Override
  public CassandraClient connect(Handler<AsyncResult<Void>> connectHandler) {
    return connect(null, connectHandler);
  }

  @Override
  public CassandraClient connect(String keyspace, Handler<AsyncResult<Void>> connectHandler) {
    cassandraHolder.session.set(null);
    Cluster.Builder builder = cassandraHolder.options.dataStaxClusterBuilder();
    if (builder.getContactPoints().isEmpty()) {
      builder.addContactPoint(CassandraClientOptions.DEFAULT_HOST);
    }
    Cluster build = builder.build();
    ListenableFuture<Session> connectGuavaFuture;
    if (keyspace == null) {
      connectGuavaFuture = build.connectAsync();
    } else {
      connectGuavaFuture = build.connectAsync(keyspace);
    }

    handleOnContext(connectGuavaFuture, vertx.getOrCreateContext(), ar -> {
      if (ar.succeeded()) {
        cassandraHolder.session.set(ar.result());
        if (connectHandler != null) {
          connectHandler.handle(Future.succeededFuture());
        }
      } else {
        if (connectHandler != null) {
          connectHandler.handle(Future.failedFuture(ar.cause()));
        }
      }
    });

    return this;
  }

  @Override
  public CassandraClient executeWithFullFetch(String query, Handler<AsyncResult<List<Row>>> resultHandler) {
    return executeWithFullFetch(new SimpleStatement(query), resultHandler);
  }

  @Override
  public CassandraClient executeWithFullFetch(Statement statement, Handler<AsyncResult<List<Row>>> resultHandler) {
    execute(statement, ar -> {
      if (ar.succeeded()) {
        ar.result().all(resultHandler);
      } else {
        resultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
    return this;
  }

  @Override
  public CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler) {
    return execute(new SimpleStatement(query), resultHandler);
  }

  @Override
  public CassandraClient execute(Statement statement, Handler<AsyncResult<ResultSet>> resultHandler) {
    Context context = vertx.getOrCreateContext();
    executeWithSession(session -> {
      handleOnContext(session.executeAsync(statement), context, ar -> {
        if (ar.succeeded()) {
          resultHandler.handle(Future.succeededFuture(new ResultSetImpl(ar.result(), vertx)));
        } else {
          resultHandler.handle(Future.failedFuture(ar.cause()));
        }
      });
    }, resultHandler);
    return this;
  }

  @Override
  public CassandraClient disconnect() {
    return disconnect(null);
  }

  @Override
  public CassandraClient prepare(String query, Handler<AsyncResult<PreparedStatement>> resultHandler) {
    Context context = vertx.getOrCreateContext();
    executeWithSession(session -> {
      handleOnContext(session.prepareAsync(query), context, ar -> {
        if (ar.succeeded()) {
          resultHandler.handle(Future.succeededFuture(ar.result()));
        } else {
          resultHandler.handle(Future.failedFuture(ar.cause()));
        }
      });
    }, resultHandler);
    return this;
  }

  @Override
  public CassandraClient queryStream(String sql, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    return queryStream(new SimpleStatement(sql), rowStreamHandler);
  }

  @Override
  public CassandraClient queryStream(Statement statement, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    Context context = vertx.getOrCreateContext();
    executeWithSession(session -> {
      handleOnContext(session.executeAsync(statement), context, ar -> {
        if (ar.succeeded()) {
          rowStreamHandler.handle(Future.succeededFuture(new CassandraRowStreamImpl(ar.result(), context)));
        } else {
          rowStreamHandler.handle(Future.failedFuture(ar.cause()));
        }
      });
    }, rowStreamHandler);
    return this;
  }

  @Override
  public CassandraClient disconnect(Handler<AsyncResult<Void>> disconnectHandler) {
    Context context = vertx.getOrCreateContext();
    executeWithSession(session -> {
      handleOnContext(session.closeAsync(), context, disconnectHandler);
    }, disconnectHandler);
    return this;
  }

  private <T> void executeWithSession(Consumer<Session> sessionConsumer, Handler<AsyncResult<T>> handlerToFailIfNoSessionPresent) {
    Session session = this.cassandraHolder.session.get();
    if (session != null) {
      sessionConsumer.accept(session);
    } else {
      if (handlerToFailIfNoSessionPresent != null) {
        handlerToFailIfNoSessionPresent.handle(Future.failedFuture("In order to do this, you should be connected"));
      }
    }
  }

  Session getSession() {
    return cassandraHolder.session.get();
  }
}
