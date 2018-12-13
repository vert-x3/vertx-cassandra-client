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

import com.datastax.driver.mapping.Mapper;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * A Vert.x wrapped datastax Mapper {@link com.datastax.driver.mapping.Mapper}.
 *
 * @author Martijn Zwennes
 */
@VertxGen
public interface VertxMapper<T> {

  /**
   * @return the underlying datastax Mapper.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Mapper<T> getMapper();

  /**
   * Asynchronous save method.
   * @param entity object to be stored in database
   * @param handler result handler
   */
  void save(T entity, Handler<AsyncResult<Void>> handler);

  /**
   * Asynchronous delete method.

   * @param entity object to be deleted
   * @param handler result handler
   */
  void delete(T entity, Handler<AsyncResult<Void>> handler);

  /**
   * Asynchronous delete method based on the primary key(s).
   * @param primaryKeys primary key(s) used to find row(s) to delete
   * @param handler result handler
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  void delete(List<Object> primaryKeys, Handler<AsyncResult<Void>> handler);

  /**
   * Asynchronous get method.
   * @param primaryKeys primary key(s) used to retrieve row(s)
   * @param handler result handler
   */
  void get(List<Object> primaryKeys, Handler<AsyncResult<T>> handler);
}
