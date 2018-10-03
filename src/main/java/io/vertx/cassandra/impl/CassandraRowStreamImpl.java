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
import com.datastax.driver.core.Row;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.queue.Queue;

import java.util.Iterator;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraRowStreamImpl implements CassandraRowStream {

  private final ResultSet datastaxResultSet;
  private final Vertx vertx;
  private final Iterator<com.datastax.driver.core.Row> resultSetIterator;
  private final Queue<Row> internalQueue;
  private final Context context;

  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;

  public CassandraRowStreamImpl(ResultSet result, Vertx vertx) {
    this.vertx = vertx;
    datastaxResultSet = result;
    resultSetIterator = result.iterator();
    context = vertx.getOrCreateContext();
    internalQueue = Queue.queue(context);
    internalQueue.writableHandler(v -> fire());
  }

  @Override
  public CassandraRowStream exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    internalQueue.exceptionHandler(handler);
    return this;
  }

  @Override
  public CassandraRowStream handler(Handler<Row> handler) {
    internalQueue.handler(handler);
    fire();
    return this;
  }

  @Override
  public CassandraRowStream pause() {
    internalQueue.pause();
    return this;
  }

  @Override
  public CassandraRowStream resume() {
    internalQueue.resume();
    return this;
  }

  @Override
  public synchronized CassandraRowStream endHandler(Handler<Void> handler) {
    endHandler = handler;
    tryToTriggerEndOfTheStream();
    return this;
  }

  @Override
  public synchronized CassandraRowStream fetch(long l) {
    internalQueue.take(l);
    return this;
  }

  private synchronized void fire() {
    int availableWithoutFetching = datastaxResultSet.getAvailableWithoutFetching();
    boolean isFetched = datastaxResultSet.isFullyFetched();
    if (availableWithoutFetching != 0) {
      for (int i = 0; i < availableWithoutFetching && internalQueue.isWritable(); i++) {
        internalQueue.add(resultSetIterator.next());
      }
      if (internalQueue.isWritable()) {
        fetchAndCallOneMoreTime();
      }
    } else if (isFetched) {
      tryToTriggerEndOfTheStream();
    } else {
      fetchAndCallOneMoreTime();
    }
  }

  private void fetchAndCallOneMoreTime() {
    fetch().setHandler(v -> {
      if (v.succeeded()) {
        fire();
      } else {
        if (exceptionHandler != null) {
          exceptionHandler.handle(v.cause());
        }
        if (endHandler != null) {
          endHandler.handle(null);
        }
      }
    });
  }

  private Future<Void> fetch() {
    if (datastaxResultSet.isFullyFetched()) {
      return Future.succeededFuture(null);
    } else {
      return Util.toVertxFuture(datastaxResultSet.fetchMoreResults(), vertx).mapEmpty();
    }
  }

  private void tryToTriggerEndOfTheStream() {
    if (endHandler != null && datastaxResultSet.isFullyFetched() && !resultSetIterator.hasNext()) {
      endHandler.handle(null);
    }
  }
}
