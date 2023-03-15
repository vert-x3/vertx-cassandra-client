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
package io.vertx.cassandra;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * A {@link ReadStream} for {@link Row} consumption.
 *
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@VertxGen
public interface CassandraRowStream extends ReadStream<Row> {

  @Override
  CassandraRowStream exceptionHandler(Handler<Throwable> handler);

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Override
  CassandraRowStream handler(Handler<Row> handler);

  @Override
  CassandraRowStream pause();

  @Override
  CassandraRowStream resume();

  @Override
  CassandraRowStream endHandler(Handler<Void> handler);

  @Override
  CassandraRowStream fetch(long l);

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Override
  default Future<Void> pipeTo(WriteStream<Row> dst) {
    return ReadStream.super.pipeTo(dst);
  }

  /**
   * Get the {@link ExecutionInfo} provided by the backing {@link ResultSet} for this stream.
   *
   * @returns the executionInfo
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  ExecutionInfo executionInfo();


  /**
   * Get the {@link ColumnDefinitions} provided by the backing {@link ResultSet} for this stream.
   *
   * @returns the columnDefinitions
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  ColumnDefinitions columnDefinitions();
}
