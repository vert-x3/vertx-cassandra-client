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
package io.vertx.cassandra.impl;

import com.datastax.driver.mapping.Mapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.cassandra.VertxMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.List;

/**
 * @author Martijn Zwennes
 */
public class VertxMapperImpl<T> implements VertxMapper<T> {

  private final Mapper<T> mapper;
  private final Vertx vertx;

  VertxMapperImpl(Mapper<T> mapper, Vertx vertx) {
    this.mapper = mapper;
    this.vertx = vertx;
  }

  @Override
  public Mapper<T> getMapper() {
    return mapper;
  }

  @Override
  public void save(T entity, Handler<AsyncResult<Void>> handler) {
    ListenableFuture<Void> futureSave = mapper.saveAsync(entity);
    handle(futureSave, handler, vertx);
  }

  @Override
  public void delete(T entity, Handler<AsyncResult<Void>> handler) {
    ListenableFuture<Void> futureDelete = mapper.deleteAsync(entity);
    handle(futureDelete, handler, vertx);
  }

  @Override
  public void delete(List<Object> primaryKeys, Handler<AsyncResult<Void>> handler) {
    ListenableFuture<Void> futureDelete = mapper.deleteAsync(primaryKeys.toArray());
    handle(futureDelete, handler, vertx);
  }

  @Override
  public void get(List<Object> primaryKeys, Handler<AsyncResult<T>> handler) {
    ListenableFuture<T> futureGet = mapper.getAsync(primaryKeys.toArray());
    handle(futureGet, handler, vertx);
  }

  private static <V> void handle(final ListenableFuture<V> future, Handler<AsyncResult<V>> handler, Vertx vertx) {
    final Context context = vertx.getOrCreateContext();
    Util.handleOnContext(future, context, handler);
  }

}
