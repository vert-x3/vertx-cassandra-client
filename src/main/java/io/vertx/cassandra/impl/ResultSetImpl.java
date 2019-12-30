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

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.cassandra.impl.Util.handleOnContext;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class ResultSetImpl implements ResultSet {

  private final Vertx vertx;
  private final AtomicReference<com.datastax.oss.driver.api.core.cql.AsyncResultSet> resultSetRef;

  public ResultSetImpl(com.datastax.oss.driver.api.core.cql.AsyncResultSet resultSet, Vertx vertx) {
    this.resultSetRef = new AtomicReference<>(resultSet);
    this.vertx = vertx;
  }

  @Override
  public ResultSet all(Handler<AsyncResult<List<Row>>> handler) {
    loadMore(vertx.getOrCreateContext(), Collections.emptyList(), handler);
    return this;
  }

  @Override
  public Future<List<Row>> all() {
    Promise<List<Row>> promise = Promise.promise();
    all(promise);
    return promise.future();
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return resultSetRef.get().getColumnDefinitions();
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return resultSetRef.get().getExecutionInfo();
  }

  @Override
  public int remaining() {
    return resultSetRef.get().remaining();
  }

  @Override
  public Iterable<Row> currentPage() {
    return resultSetRef.get().currentPage();
  }

  @Override
  public Row one() {
    return resultSetRef.get().one();
  }

  @Override
  public boolean hasMorePages() {
    return resultSetRef.get().hasMorePages();
  }

  @Override
  public Future<ResultSet> fetchNextPage() throws IllegalStateException {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<ResultSet> promise = ctx.promise();
    Util.handleOnContext(resultSetRef.get().fetchNextPage(), ctx, datastaxRS -> {
      resultSetRef.set(datastaxRS);
      return this;
    }, promise);
    return promise.future();
  }

  @Override
  public boolean wasApplied() {
    return resultSetRef.get().wasApplied();
  }

  private void loadMore(Context context, List<Row> loaded, Handler<AsyncResult<List<Row>>> handler) {
    int availableWithoutFetching = resultSetRef.get().remaining();
    List<Row> rows = new ArrayList<>(loaded.size() + availableWithoutFetching);
    rows.addAll(loaded);
    for (int i = 0; i < availableWithoutFetching; i++) {
      rows.add(resultSetRef.get().one());
    }

    if (resultSetRef.get().hasMorePages()) {
      handleOnContext(resultSetRef.get().fetchNextPage(), context, ar -> {
        if (ar.succeeded()) {
          resultSetRef.set(ar.result());
          loadMore(context, rows, handler);
        } else {
          if (handler != null) {
            handler.handle(Future.failedFuture(ar.cause()));
          }
        }
      });
    } else {
      if (handler != null) {
        handler.handle(Future.succeededFuture(rows));
      }
    }
  }
}
