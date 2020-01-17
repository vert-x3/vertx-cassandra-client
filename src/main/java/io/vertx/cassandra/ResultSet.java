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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;

/**
 * It is like {@link com.datastax.oss.driver.api.core.cql.AsyncResultSet}, but adapted for Vert.x.
 *
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@VertxGen
public interface ResultSet {

  /**
   * Like {@link #all(Handler)} but returns a {@code Future} of the asynchronous result.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<Row>> all();

  /**
   * The method should <strong>not</strong> be used concurrently with others like {@link #fetchNextPage()} or {@link #one(Handler)}.
   * This may lead to unexpected result.
   *
   * @param handler handler called when all the rows is fetched
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  ResultSet all(Handler<AsyncResult<List<Row>>> handler);

  /**
   * @see AsyncResultSet#getColumnDefinitions()
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  ColumnDefinitions getColumnDefinitions();

  /**
   * @see AsyncResultSet#getExecutionInfo()
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  ExecutionInfo getExecutionInfo();

  /**
   * @see AsyncResultSet#remaining()
   */
  int remaining();

  /**
   * @see AsyncResultSet#currentPage()
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Iterable<Row> currentPage();

  /**
   * @see AsyncResultSet#one()
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Row one();

  /**
   * @see AsyncResultSet#hasMorePages()
   */
  boolean hasMorePages();

  /**
   * @see AsyncResultSet#wasApplied()
   */
  Future<ResultSet> fetchNextPage() throws IllegalStateException;

  /**
   * @see AsyncResultSet#wasApplied()
   */
  boolean wasApplied();
}
