package io.vertx.cassandra.impl;

import com.datastax.driver.core.ResultSet;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.Row;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CassandraRowStreamImpl implements CassandraRowStream {

  private final ResultSet datastaxResultSet;
  private final Vertx vertx;
  private final Iterator<com.datastax.driver.core.Row> resultSetIterator;
  private final Lock iteratorAccessLock = new ReentrantLock();
  private final Context context;

  private volatile Handler<Throwable> exceptionHandler;
  private volatile Handler<Row> rowHandler;
  private volatile Handler<Void> endHandler;

  private AtomicBoolean paused = new AtomicBoolean(false);

  public CassandraRowStreamImpl(ResultSet result, Vertx vertx) {
    this.vertx = vertx;
    datastaxResultSet = result;
    resultSetIterator = result.iterator();
    context = vertx.getOrCreateContext();
  }

  @Override
  public ReadStream<Row> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Row> handler(Handler<Row> handler) {
    rowHandler = handler;
    if (handler != null) {
      fireStream();
    }
    return this;
  }

  @Override
  public ReadStream<Row> pause() {
    paused.set(true);
    return this;
  }

  @Override
  public ReadStream<Row> resume() {
    paused.set(false);
    fireStream();
    return this;
  }

  @Override
  public ReadStream<Row> endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  private void fireStream() {
    if (!paused.get() && rowHandler != null) {
      int availableWithoutFetching = datastaxResultSet.getAvailableWithoutFetching();
      for (int i = 0; i < availableWithoutFetching; i++) {

        iteratorAccessLock.lock();
        Row row = new RowImpl(resultSetIterator.next());
        iteratorAccessLock.unlock();

        context.runOnContext(v -> rowHandler.handle(row));
        if (i == availableWithoutFetching - 1) {
          context.runOnContext(v -> {
            rowHandler.handle(row);
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
          });
        } else {
          context.runOnContext(v -> rowHandler.handle(row));
        }
      }
    }
  }

  private void tryToTriggerEndOfTheStream() {
    iteratorAccessLock.lock();
    if (endHandler != null && datastaxResultSet.isFullyFetched() && !resultSetIterator.hasNext()) {
      endHandler.handle(null);
    }
    iteratorAccessLock.unlock();
  }
}
