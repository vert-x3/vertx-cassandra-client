/*
 * Copyright 2024 The Vert.x Community.
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
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.EventExecutor;
import io.vertx.core.internal.concurrent.InboundMessageQueue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
public class CassandraRowStreamImpl implements CassandraRowStream {

  private static final Object DONE = new Object();

  private final ContextInternal context;
  private final Queue internalQueue;

  private Handler<Row> rowHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;

  private ExecutionInfo executionInfo;
  private ColumnDefinitions columnDefinitions;

  public CassandraRowStreamImpl(Context context) {

    Queue queue = new Queue((ContextInternal) context);
    queue.pause();

    this.context = (ContextInternal) context;
    this.internalQueue = queue;
  }

  void init(ResultSet resultSet) {
    executionInfo = resultSet.getExecutionInfo();
    columnDefinitions = resultSet.getColumnDefinitions();
    internalQueue.init(resultSet);
  }

  @Override
  public synchronized CassandraRowStream exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public CassandraRowStream handler(Handler<Row> handler) {
    synchronized (this) {
      rowHandler = handler;
    }
    if (handler == null) {
      pause();
    } else {
      resume();
    }
    return this;
  }

  @Override
  public synchronized CassandraRowStream endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  @Override
  public CassandraRowStream pause() {
    internalQueue.pause();
    return this;
  }

  @Override
  public CassandraRowStream resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public CassandraRowStream fetch(long l) {
    internalQueue.fetch(l);
    return this;
  }

  @Override
  public ExecutionInfo executionInfo() {
    return executionInfo;
  }

  @Override
  public ColumnDefinitions columnDefinitions() {
    return columnDefinitions;
  }

  private final Lock lock = new ReentrantLock();
  private final EventExecutor executor = new EventExecutor() {
    @Override
    public boolean inThread() {
      return true;
    }
    @Override
    public void execute(Runnable command) {
      lock.lock();
      try {
        command.run();
      } finally {
        lock.unlock();
      }
    }
  };

  private class Queue extends InboundMessageQueue<Object> {

    private ResultSet resultSet;
    private boolean paused;

    public Queue(ContextInternal context) {
      super(executor, context.executor());
    }

    void init(ResultSet rs) {
      transfer(rs);
    }

    private void transfer(ResultSet rs) {
      Iterable<Row> page = rs.currentPage();
      lock.lock();
      try {
        for (Row row : page) {
          write(new StreamItem(rs, row));
        }
      } finally {
        lock.unlock();
      }
      if (rs.hasMorePages()) {
        Future<ResultSet> next = rs.fetchNextPage();
        next.onComplete((res, err) -> {
          if (err == null) {
            resultSet = res;
            if (!paused) {
              transfer(res);
            }
          } else {
            write(err);
          }
        });
      } else {
        write(DONE);
      }
    }

    @Override
    protected void handleResume() {
      paused = false;
      ResultSet rs = resultSet;
      resultSet = null;
      if (rs != null) {
        transfer(rs);
      }
    }

    @Override
    protected void handlePause() {
      paused = true;
    }

    @Override
    protected void handleMessage(Object msg) {
      if (msg == DONE) {
        Handler<Void> handler;
        synchronized (CassandraRowStreamImpl.this) {
          handler = endHandler;
        }
        if (handler != null) {
          context.emit(null, handler);
        }
      } else if (msg instanceof StreamItem) {
        StreamItem item = (StreamItem) msg;
        Handler<Row> handler;
        synchronized (CassandraRowStreamImpl.this) {
          handler = rowHandler;
        }
        executionInfo = item.executionInfo;
        columnDefinitions = item.columnDefinitions;
        if (handler != null) {
          context.emit(item.row, handler);
        }
      } else if (msg instanceof Throwable) {
        Throwable err = (Throwable) msg;
        Handler<Throwable> handler;
        synchronized (CassandraRowStreamImpl.this) {
          handler = exceptionHandler;
        }
        if (handler != null) {
          context.emit(err, handler);
        }
      }
    }
  }

  private static class StreamItem {
    public final ExecutionInfo executionInfo;
    public final ColumnDefinitions columnDefinitions;
    public final Row row;

    StreamItem(ResultSet resultSet, Row row) {
      this.executionInfo = resultSet.getExecutionInfo();
      this.columnDefinitions = resultSet.getColumnDefinitions();
      this.row = row;
    }
  }
}
