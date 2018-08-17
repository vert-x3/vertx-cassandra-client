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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.EventLoopGroup;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraClientImpl implements CassandraClient {

  private static final String DS_LOCAL_MAP_NAME = "__vertx.CassandraClient.datasources";

  VertxInternal vertx;
  final CassandraHolder cassandraHolder;

  public CassandraClientImpl(Vertx vertx, String dataSourceName, CassandraClientOptions cassandraClientOptions) {
    this.vertx = (VertxInternal) vertx;
    this.cassandraHolder = lookupHolder(dataSourceName, cassandraClientOptions);
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
    public synchronized void close() {
      if (--refCount == 0) {
        if (session.get() != null) {
          session.get().close();
        }
        if (closeRunner != null) {
          closeRunner.run();
        }
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
  public CassandraClient connect(Handler<AsyncResult<Void>> connectHandler) {
    return connect(null, connectHandler);
  }

  @Override
  public CassandraClient connect(String keyspace, Handler<AsyncResult<Void>> connectHandler) {
    cassandraHolder.session.set(null);
    Cluster.Builder builder = Cluster.builder();

    if (cassandraHolder.options.contactPoints().isEmpty()) {
      builder.addContactPoint(CassandraClientOptions.DEFAULT_HOST);
    } else {
      for (String contactPoint : cassandraHolder.options.contactPoints()) {
        builder.addContactPoint(contactPoint);
      }
    }

    Cluster build = builder
      .withNettyOptions(new VertxNettyOptions(vertx))
      .withPort(cassandraHolder.options.port())
      .build();
    ListenableFuture<Session> connectGuavaFuture;
    if (keyspace == null) {
      connectGuavaFuture = build.connectAsync();
    } else {
      connectGuavaFuture = build.connectAsync(keyspace);
    }

    Future<Session> sessionFuture = Util.toVertxFuture(connectGuavaFuture, vertx);

    sessionFuture.setHandler(event -> {
      if (event.succeeded()) {
        cassandraHolder.session.set(event.result());
        if (connectHandler != null) {
          connectHandler.handle(Future.succeededFuture());
        }
      } else {
        if (connectHandler != null) {
          connectHandler.handle(Future.failedFuture(event.cause()));
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
    Future<ResultSet> resultSetFuture = Future.future();
    execute(statement, resultSetFuture);
    resultSetFuture.compose(resultSet -> {
      Future<List<Row>> rowsFuture = Future.future();
      resultSet.all(rowsFuture);
      return rowsFuture;
    }).setHandler(resultHandler);
    return this;
  }

  @Override
  public CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler) {
    return execute(new SimpleStatement(query), resultHandler);
  }

  @Override
  public CassandraClient execute(Statement statement, Handler<AsyncResult<ResultSet>> resultHandler) {
    executeWithSession(session -> {
      Future<com.datastax.driver.core.ResultSet> future = Util.toVertxFuture(session.executeAsync(statement), vertx);
      future.setHandler(executed -> {
        if (executed.succeeded()) {
          if (resultHandler != null) {
            resultHandler.handle(Future.succeededFuture(new ResultSetImpl(executed.result(), vertx)));
          }
        } else {
          if (resultHandler != null) {
            resultHandler.handle(Future.failedFuture(executed.cause()));
          }
        }
      });
      return null;
    }, resultHandler);
    return this;
  }

  @Override
  public CassandraClient disconnect() {
    return disconnect(null);
  }

  @Override
  public CassandraClient prepare(String query, Handler<AsyncResult<PreparedStatement>> resultHandler) {
    executeWithSession(session -> {
      ListenableFuture<com.datastax.driver.core.PreparedStatement> preparedFuture = session.prepareAsync(query);
      Future<com.datastax.driver.core.PreparedStatement> vertxExecuteFuture = Util.toVertxFuture(preparedFuture, vertx);
      vertxExecuteFuture.setHandler(executionResult -> {
        if (executionResult.succeeded()) {
          if (resultHandler != null) {
            resultHandler.handle(Future.succeededFuture(executionResult.result()));
          }
        } else {
          if (resultHandler != null) {
            resultHandler.handle(Future.failedFuture(executionResult.cause()));
          }
        }
      });
      return null;
    }, resultHandler);
    return this;
  }

  @Override
  public CassandraClient queryStream(String sql, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    return queryStream(new SimpleStatement(sql), rowStreamHandler);
  }

  @Override
  public CassandraClient queryStream(Statement statement, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler) {
    executeWithSession(session -> {
      ResultSetFuture resultSetFuture = session.executeAsync(statement);
      Future<com.datastax.driver.core.ResultSet> vertxExecuteFuture = Util.toVertxFuture(resultSetFuture, vertx);
      vertxExecuteFuture.setHandler(executionResult -> {
        if (executionResult.succeeded()) {
          if (rowStreamHandler != null) {
            rowStreamHandler.handle(Future.succeededFuture(new CassandraRowStreamImpl(executionResult.result(), vertx)));
          }
        } else {
          if (rowStreamHandler != null) {
            rowStreamHandler.handle(Future.failedFuture(executionResult.cause()));
          }
        }
      });
      return null;
    }, rowStreamHandler);
    return this;
  }

  @Override
  public CassandraClient disconnect(Handler<AsyncResult<Void>> disconnectHandler) {
    executeWithSession(session -> {
      Util.toVertxFuture(session.closeAsync(), vertx).<Void>mapEmpty().setHandler(disconnectHandler);
      return null;
    }, disconnectHandler);
    return this;
  }

  private <T> void executeWithSession(Function<Session, Void> functionToExecute, Handler<AsyncResult<T>> handlerToFailIfNoSessionPresent) {
    Session session = this.cassandraHolder.session.get();
    if (session != null) {
      functionToExecute.apply(session);
    } else {
      if (handlerToFailIfNoSessionPresent != null) {
        handlerToFailIfNoSessionPresent.handle(Future.failedFuture("In order to do this, you should be connected"));
      }
    }
  }

  private static class VertxNettyOptions extends NettyOptions {

    VertxInternal vertx;

    public VertxNettyOptions(VertxInternal vertx) {
      this.vertx = vertx;
    }

    @Override
    public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory) {
      return vertx.getEventLoopGroup();
    }

    @Override
    public void onClusterClose(EventLoopGroup eventLoopGroup) {
      // it is important to not do anything here
      // because the default behaviour is to shutdown the Vert.x event loop group
    }
  }
}
