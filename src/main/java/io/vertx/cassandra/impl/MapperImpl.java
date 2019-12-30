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

import io.vertx.cassandra.Mapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.cassandra.impl.Util.setHandler;
import static io.vertx.cassandra.impl.Util.toVertxFuture;

/**
 * @author Martijn Zwennes
 */
public class MapperImpl<T> implements Mapper<T> {

  private final MappingManagerImpl mappingManager;
  private final Class<T> mappedClass;
  private final VertxInternal vertx;

  private AtomicReference<com.datastax.driver.mapping.Mapper<T>> mapper = new AtomicReference<>();

  MapperImpl(MappingManagerImpl mappingManager, Class<T> mappedClass) {
    Objects.requireNonNull(mappingManager, "mappingManager");
    Objects.requireNonNull(mappedClass, "mappedClass");
    this.mappingManager = mappingManager;
    this.mappedClass = mappedClass;
    vertx = mappingManager.client.vertx;
  }

  @Override
  public void save(T entity, Handler<AsyncResult<Void>> handler) {
    Future<Void> future = save(entity);
    setHandler(future, handler);
  }

  @Override
  public Future<Void> save(T entity) {
    ContextInternal context = vertx.getOrCreateContext();
    return getMapper(context)
      .flatMap(m -> Util.toVertxFuture(m.saveAsync(entity), context));
  }

  @Override
  public void delete(List<Object> primaryKey, Handler<AsyncResult<Void>> handler) {
    Future<Void> future = delete(primaryKey);
    setHandler(future, handler);
  }

  @Override
  public Future<Void> delete(List<Object> primaryKey) {
    ContextInternal context = vertx.getOrCreateContext();
    return getMapper(context)
      .flatMap(m -> toVertxFuture(m.deleteAsync(primaryKey.toArray()), context));
  }

  @Override
  public void get(List<Object> primaryKey, Handler<AsyncResult<T>> handler) {
    Future<T> future = get(primaryKey);
    setHandler(future, handler);
  }

  @Override
  public Future<T> get(List<Object> primaryKey) {
    ContextInternal context = vertx.getOrCreateContext();
    return getMapper(context)
      .flatMap(m -> toVertxFuture(m.getAsync(primaryKey.toArray()), context));
  }

  private synchronized Future<com.datastax.driver.mapping.Mapper<T>> getMapper(ContextInternal context) {
    com.datastax.driver.mapping.Mapper<T> current = mapper.get();
    if (current != null) {
      return context.succeededFuture(current);
    }
    return mappingManager.getMappingManager(context)
      .map(this::getOrCreateMapper);
  }

  private com.datastax.driver.mapping.Mapper<T> getOrCreateMapper(com.datastax.driver.mapping.MappingManager manager) {
    return mapper.updateAndGet(m -> m != null ? m : manager.mapper(mappedClass));
  }
}
