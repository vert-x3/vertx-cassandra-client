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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * It is like {@link com.datastax.driver.core.ResultSet}, but adapted for Vert.x.
 *
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@VertxGen
public interface ResultSet {

  /**
   * @see com.datastax.driver.core.ResultSet#isExhausted()
   */
  boolean isExhausted();

  /**
   * @see com.datastax.driver.core.ResultSet#isFullyFetched()
   */
  boolean isFullyFetched();

  /**
   * @see com.datastax.driver.core.ResultSet#getAvailableWithoutFetching()
   */
  int getAvailableWithoutFetching();

  /**
   * @param handler handler called when result is fetched
   * @see com.datastax.driver.core.ResultSet#fetchMoreResults()
   */
  @Fluent
  ResultSet fetchMoreResults(Handler<AsyncResult<Void>> handler);

  /**
   * @param handler handler called when one row is fetched
   * @see com.datastax.driver.core.ResultSet#one
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  ResultSet one(Handler<AsyncResult<@Nullable Row>> handler);

  /**
   * Fetch a specific amount of rows and notify via a handler.
   *
   * @param handler the handler
   * @param amount the amount
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  ResultSet several(int amount, Handler<AsyncResult<List<Row>>> handler);

  /**
   * @param handler handler called when all the rows is fetched
   * @see com.datastax.driver.core.ResultSet#all
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  ResultSet all(Handler<AsyncResult<List<Row>>> handler);

  /**
   * @see com.datastax.driver.core.ResultSet#getColumnDefinitions
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  ColumnDefinitions getColumnDefinitions();

  /**
   * @see com.datastax.driver.core.ResultSet#wasApplied
   */
  boolean wasApplied();
}
