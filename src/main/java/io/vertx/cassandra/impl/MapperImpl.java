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
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;

import java.util.List;
import java.util.Objects;

import static io.vertx.cassandra.impl.Util.handleOnContext;

/**
 * @author Martijn Zwennes
 */
public class MapperImpl<T> implements Mapper<T> {

  private final MappingManagerImpl mappingManager;
  private final Class<T> mappedClass;
  private com.datastax.driver.mapping.Mapper<T> mapper;

  MapperImpl(MappingManagerImpl mappingManager, Class<T> mappedClass) {
    Objects.requireNonNull(mappingManager, "mappingManager");
    Objects.requireNonNull(mappedClass, "mappedClass");
    this.mappingManager = mappingManager;
    this.mappedClass = mappedClass;
  }

  @Override
  public void save(T entity, Handler<AsyncResult<Void>> handler) {
    ContextInternal context = mappingManager.client.vertx.getOrCreateContext();
    getMapper(context, ar -> {
      if (ar.succeeded()) {
        handleOnContext(mapper.saveAsync(entity), context, handler);
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public Future<Void> save(T entity) {
    Promise<Void> promise = Promise.promise();
    save(entity, promise);
    return promise.future();
  }

  @Override
  public void delete(List<Object> primaryKey, Handler<AsyncResult<Void>> handler) {
    ContextInternal context = mappingManager.client.vertx.getOrCreateContext();
    getMapper(context, ar -> {
      if (ar.succeeded()) {
        handleOnContext(mapper.deleteAsync(primaryKey.toArray()), context, handler);
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public Future<Void> delete(List<Object> primaryKey) {
    Promise<Void> promise = Promise.promise();
    delete(primaryKey, promise);
    return promise.future();
  }

  @Override
  public void get(List<Object> primaryKey, Handler<AsyncResult<T>> handler) {
    ContextInternal context = mappingManager.client.vertx.getOrCreateContext();
    getMapper(context, ar -> {
      if (ar.succeeded()) {
        handleOnContext(mapper.getAsync(primaryKey.toArray()), context, handler);
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public Future<T> get(List<Object> primaryKey) {
    Promise<T> promise = Promise.promise();
    get(primaryKey, promise);
    return promise.future();
  }

  synchronized void getMapper(ContextInternal context, Handler<AsyncResult<com.datastax.driver.mapping.Mapper>> handler) {
    if (mapper != null) {
      handler.handle(Future.succeededFuture(mapper));
    } else {
      mappingManager.getMappingManager(context, ar -> {
        if (ar.succeeded()) {
          com.datastax.driver.mapping.Mapper m;
          try {
            synchronized (this) {
              if (mapper == null) {
                mapper = ar.result().mapper(mappedClass);
              }
              m = mapper;
            }
          } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
          }
          handler.handle(Future.succeededFuture(m));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    }
  }
}
