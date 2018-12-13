/*
 * Copyright 2019 The Vert.x Community.
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

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * It is like {@link com.datastax.driver.mapping.Mapper}, but adapted for Vert.x.
 *
 * @author Martijn Zwennes
 */
@VertxGen
public interface Mapper<T> {

  /**
   * Asynchronous save method.
   *
   * @param entity object to be stored in database
   * @param handler result handler
   */
  void save(T entity, Handler<AsyncResult<Void>> handler);

  /**
   * Asynchronous delete method based on the column values of the primary key.
   *
   * @param primaryKey primary key used to find row to delete
   * @param handler result handler
   */
  void delete(List<Object> primaryKey, Handler<AsyncResult<Void>> handler);

  /**
   * Asynchronous get method based on the column values of the primary key.
   *
   * @param primaryKey primary key used to retrieve row
   * @param handler result handler
   */
  void get(List<Object> primaryKey, Handler<AsyncResult<T>> handler);
}
