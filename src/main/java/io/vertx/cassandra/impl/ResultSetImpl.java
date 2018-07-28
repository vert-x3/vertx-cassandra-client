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
import com.datastax.driver.core.Row;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class ResultSetImpl implements ResultSet {

  private com.datastax.driver.core.ResultSet resultSet;
  private Vertx vertx;

  public ResultSetImpl(com.datastax.driver.core.ResultSet resultSet, Vertx vertx) {
    this.resultSet = resultSet;
    this.vertx = vertx;
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
    Util.toVertxFuture(resultSet.fetchMoreResults(), vertx).<Void>mapEmpty().setHandler(handler);
    return this;
  }

  @Override
  public ResultSet one(Handler<AsyncResult<Row>> handler) {
    if (getAvailableWithoutFetching() == 0 && !resultSet.isFullyFetched()) {
      Util.toVertxFuture(resultSet.fetchMoreResults(), vertx).setHandler(done -> {
        if (done.succeeded()) {
          if (handler != null) {
            handler.handle(Future.succeededFuture(resultSet.one()));
          }
        } else {
          if (handler != null) {
            handler.handle(Future.failedFuture(done.cause()));
          }
        }
      });
    } else {
      handler.handle(Future.succeededFuture(resultSet.one()));
    }
    return this;
  }

  @Override
  public ResultSet all(Handler<AsyncResult<List<Row>>> handler) {
    ArrayList<Row> rows = new ArrayList<>();
    new Fetcher(resultSet, rows)
      .fetched()
      .setHandler(grabbed -> {
        if (grabbed.succeeded()) {
          handler.handle(Future.succeededFuture(rows));
        } else {
          handler.handle(Future.failedFuture(grabbed.cause()));
        }
      });
    return this;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return resultSet.getColumnDefinitions();
  }

  @Override
  public boolean wasApplied() {
    return resultSet.wasApplied();
  }

  /**
   * Helper class used for fetching results and filling a provided list.
   */
  private class Fetcher {

    private com.datastax.driver.core.ResultSet resultSet;
    private List<Row> listToFill;
    private Future<Void> doneFuture = Future.future();

    private Fetcher(com.datastax.driver.core.ResultSet resultSet, List<Row> listToFill) {
      this.resultSet = resultSet;
      this.listToFill = listToFill;
      fetch();
    }

    private void fetch() {
      int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
      for (int i = 0; i < availableWithoutFetching; i++) {
        listToFill.add(resultSet.one());
      }

      if (!resultSet.isFullyFetched()) {
        Util.toVertxFuture(resultSet.fetchMoreResults(), vertx)
          .setHandler(h -> {
            if (h.succeeded()) {
              fetch();
            } else {
              doneFuture.handle(Future.failedFuture(h.cause()));
            }
          });
      } else {
        doneFuture.handle(Future.succeededFuture());
      }
    }

    private Future<Void> fetched() {
      return doneFuture;
    }
  }
}
