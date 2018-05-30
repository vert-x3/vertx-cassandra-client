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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.ExecutableQuery;
import io.vertx.cassandra.PreparedQuery;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;

import java.util.concurrent.atomic.AtomicReference;

public class CassandraClientImpl implements CassandraClient {

  VertxInternal vertx;
  CassandraClientOptions options;
  AtomicReference<Session> session = new AtomicReference<>(null);

  public CassandraClientImpl() {
  }

  public CassandraClientImpl(Vertx vertx, CassandraClientOptions cassandraClientOptions) {
    this.vertx = (VertxInternal) vertx;
    this.options = cassandraClientOptions;
  }

  @Override
  public CassandraClient connect(String keyspace, Handler<AsyncResult<Void>> connectHandler) {
    session.set(null);
    Cluster.Builder builder = Cluster.builder();

    if (options.contactPoints().isEmpty()) {
      builder.addContactPoint(CassandraClientOptions.DEFAULT_HOST);
    } else {
      for (String contactPoint : options.contactPoints()) {
        builder.addContactPoint(contactPoint);
      }
    }

    Cluster build = builder.withPort(options.port()).build();
    ListenableFuture<Session> connectGuavaFuture;
    if (keyspace == null) {
      connectGuavaFuture = build.connectAsync();
    } else {
      connectGuavaFuture = build.connectAsync(keyspace);
    }

    Future<Session> sessionFuture = Util.toVertxFuture(connectGuavaFuture, vertx);

    sessionFuture.setHandler(event -> {
      if (event.succeeded()) {
        session.set(event.result());
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
  public CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler){
    return execute(ExecutableQuery.fromString(query), resultHandler);
  }

  @Override
  public CassandraClient execute(ExecutableQuery query, Handler<AsyncResult<ResultSet>> resultHandler) {
    Session session = this.session.get();
    if (session != null) {
      ResultSetFuture resultSetFuture = session.executeAsync(((ExecutableQueryImpl) query).statement);
      Future<com.datastax.driver.core.ResultSet> vertxExecuteFuture = Util.toVertxFuture(resultSetFuture, vertx);
      vertxExecuteFuture.setHandler(executionResult -> {
        if (executionResult.succeeded()) {
          if (resultHandler != null) {
            resultHandler.handle(Future.succeededFuture(new ResultSetImpl(executionResult.result())));
          }
        } else {
          if (resultHandler != null) {
            resultHandler.handle(Future.failedFuture(executionResult.cause()));
          }
        }
      });
    } else {
      if (resultHandler != null) {
        resultHandler.handle(Future.failedFuture("In order to execute the query, you should be connected"));
      }
    }
    return this;
  }

  @Override
  public CassandraClient prepare(String query, Handler<AsyncResult<PreparedQuery>> resultHandler) {
    Session session = this.session.get();
    if (session != null) {
      ListenableFuture<PreparedStatement> preparedFuture = session.prepareAsync(query);
      Future<PreparedStatement> vertxExecuteFuture = Util.toVertxFuture(preparedFuture, vertx);
      vertxExecuteFuture.setHandler(executionResult -> {
        if (executionResult.succeeded()) {
          if (resultHandler != null) {
            resultHandler.handle(Future.succeededFuture(new PreparedQueryImpl(executionResult.result())));
          }
        } else {
          if (resultHandler != null) {
            resultHandler.handle(Future.failedFuture(executionResult.cause()));
          }
        }
      });
    } else {
      if (resultHandler != null) {
        resultHandler.handle(Future.failedFuture("In order to prepare the query, you should be connected"));
      }
    }
    return this;
  }

  @Override
  public CassandraClient disconnect(Handler<AsyncResult<Void>> disconnectHandler) {
    Session session = this.session.get();
    if (session != null) {
      Future<Void> vertxFuture = Util.toVertxFuture(session.closeAsync(), vertx);
      vertxFuture.setHandler(event -> {
        if (event.succeeded()) {
          if (disconnectHandler != null) {
            disconnectHandler.handle(Future.succeededFuture());
          }
        } else {
          if (disconnectHandler != null) {
            disconnectHandler.handle(Future.failedFuture(event.cause()));
          }
        }
      });
    }
    return this;
  }
}
