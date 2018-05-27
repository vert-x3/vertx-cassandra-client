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

import com.datastax.driver.core.ResultSet;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.Row;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Iterator;

public class CassandraRowStreamImpl implements CassandraRowStream {

  private final ResultSet datastaxResultSet;
  private final Vertx vertx;
  private final Iterator<com.datastax.driver.core.Row> resultSetIterator;
  private final Context context;

  private Handler<Throwable> exceptionHandler;
  private Handler<Row> rowHandler;
  private Handler<Void> endHandler;

  private boolean paused = false;

  public CassandraRowStreamImpl(ResultSet result, Vertx vertx) {
    this.vertx = vertx;
    datastaxResultSet = result;
    resultSetIterator = result.iterator();
    context = vertx.getOrCreateContext();
  }

  @Override
  public synchronized CassandraRowStream exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public synchronized CassandraRowStream handler(Handler<Row> handler) {
    rowHandler = handler;
    if (handler != null) {
      fireStream();
    }
    return this;
  }

  @Override
  public synchronized CassandraRowStream pause() {
    paused = true;
    return this;
  }

  @Override
  public synchronized CassandraRowStream resume() {
    paused = false;
    fireStream();
    return this;
  }

  @Override
  public synchronized CassandraRowStream endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  private void fireStream() {
    if (!paused && rowHandler != null) {
      int availableWithoutFetching = datastaxResultSet.getAvailableWithoutFetching();
      for (int i = 0; i < availableWithoutFetching; i++) {

        CassandraRowStreamImpl thisStream = this;
        if (i == availableWithoutFetching - 1) {
          context.runOnContext(v -> {

            synchronized (thisStream) {
              if (!paused) {
                rowHandler.handle(new RowImpl(resultSetIterator.next()));
                tryToTriggerEndOfTheStream();
                if (!datastaxResultSet.isFullyFetched()) {
                  Future<ResultSet> fetched = Util.toVertxFuture(datastaxResultSet.fetchMoreResults(), vertx);
                  fetched.setHandler(whenFetched -> {
                    if (whenFetched.succeeded()) {
                      fireStream();
                    } else {
                      if (exceptionHandler != null) {
                        exceptionHandler.handle(whenFetched.cause());
                      }
                    }
                  });
                }
              }
            }

          });
        } else {
          context.runOnContext(v -> {
            synchronized (thisStream) {
              if (!paused) {
                rowHandler.handle(new RowImpl(resultSetIterator.next()));
              }
            }
          });
        }
      }
    }
  }

  private void tryToTriggerEndOfTheStream() {
    if (endHandler != null && datastaxResultSet.isFullyFetched() && !resultSetIterator.hasNext()) {
      endHandler.handle(null);
    }
  }
}
