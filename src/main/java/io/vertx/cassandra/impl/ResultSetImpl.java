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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import io.vertx.cassandra.ResultSet;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.vertx.cassandra.impl.Util.setHandler;
import static io.vertx.cassandra.impl.Util.toVertxFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class ResultSetImpl implements ResultSet {

  private com.datastax.driver.core.ResultSet resultSet;
  private final ContextInternal context;

  public ResultSetImpl(com.datastax.driver.core.ResultSet resultSet, ContextInternal context) {
    this.resultSet = resultSet;
    this.context = context;
  }

  @Override
  public boolean isExhausted() {
    return resultSet.isExhausted();
  }

  @Override
  public boolean isFullyFetched() {
    return resultSet.isFullyFetched();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return resultSet.getAvailableWithoutFetching();
  }

  @Override
  public ResultSet fetchMoreResults(Handler<AsyncResult<Void>> handler) {
    Future<Void> future = fetchMoreResults();
    setHandler(future, handler);
    return this;
  }

  @Override
  public Future<Void> fetchMoreResults() {
    return toVertxFuture(resultSet.fetchMoreResults(), context).mapEmpty();
  }

  @Override
  public ResultSet one(Handler<AsyncResult<Row>> handler) {
    Future<Row> future = one();
    setHandler(future, handler);
    return this;
  }

  @Override
  public Future<@Nullable Row> one() {
    if (getAvailableWithoutFetching() == 0 && !resultSet.isFullyFetched()) {
      return fetchMoreResults().map(v -> resultSet.one());
    }
    return context.succeededFuture(resultSet.one());
  }

  @Override
  public ResultSet several(int amount, Handler<AsyncResult<List<Row>>> handler) {
    Future<List<Row>> future = several(amount);
    setHandler(future, handler);
    return this;
  }

  @Override
  public Future<List<Row>> several(int amount) {
    return loadSeveral(amount);
  }

  private Future<List<Row>> loadSeveral(int count) {
    if (count <= 0) {
      return context.succeededFuture(Collections.emptyList());
    }
    int availableWithoutFetching = getAvailableWithoutFetching();
    if (availableWithoutFetching > 0 && availableWithoutFetching < count) {
      List<Row> rows = getRows(availableWithoutFetching);
      return loadSeveral(count - rows.size())
        .map(res -> concat(rows.stream(), res.stream()).collect(toList()));
    }
    if (availableWithoutFetching >= count) {
      List<Row> rows = getRows(count);
      return context.succeededFuture(rows);
    }
    if (isFullyFetched()) {
      return context.succeededFuture(Collections.emptyList());
    }
    return fetchMoreResults().flatMap(v -> loadSeveral(count));
  }

  private List<Row> getRows(int amountToFetch) {
    List<Row> rows = new ArrayList<>(amountToFetch);
    for (int i = 0; i < amountToFetch; i++) {
      Row row = resultSet.one();
      if (row != null) {
        rows.add(row);
      } else {
        break;
      }
    }
    return rows;
  }

  @Override
  public ResultSet all(Handler<AsyncResult<List<Row>>> handler) {
    Future<List<Row>> future = all();
    setHandler(future, handler);
    return this;
  }

  @Override
  public Future<List<Row>> all() {
    return loadMore();
  }

  private Future<List<Row>> loadMore() {
    List<Row> rows = getRows(resultSet.getAvailableWithoutFetching());
    if (resultSet.isFullyFetched()) {
      return context.succeededFuture(rows);
    }
    return fetchMoreResults()
      .flatMap(v -> loadMore())
      .map(res -> concat(rows.stream(), res.stream()).collect(toList()));
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return resultSet.getColumnDefinitions();
  }

  @Override
  public boolean wasApplied() {
    return resultSet.wasApplied();
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return resultSet.getExecutionInfo();
  }

  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    return resultSet.getAllExecutionInfo();
  }
}
