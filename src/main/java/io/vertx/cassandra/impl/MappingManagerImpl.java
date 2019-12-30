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

import com.datastax.driver.core.Session;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.Mapper;
import io.vertx.cassandra.MappingManager;
import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Martijn Zwennes
 */
public class MappingManagerImpl implements MappingManager {

  final CassandraClientImpl client;

  private AtomicReference<com.datastax.driver.mapping.MappingManager> mappingManager = new AtomicReference<>();

  public MappingManagerImpl(CassandraClient client) {
    Objects.requireNonNull(client, "client");
    this.client = (CassandraClientImpl) client;
  }

  @Override
  public <T> Mapper<T> mapper(Class<T> mappedClass) {
    return new MapperImpl<>(this, mappedClass);
  }

  synchronized Future<com.datastax.driver.mapping.MappingManager> getMappingManager(ContextInternal context) {
    com.datastax.driver.mapping.MappingManager current = mappingManager.get();
    if (current != null) {
      return context.succeededFuture(current);
    }
    return client.getSession(context)
      .map(this::getOrCreateManager);
  }

  private com.datastax.driver.mapping.MappingManager getOrCreateManager(Session session) {
    return mappingManager.updateAndGet(m -> m != null ? m : new com.datastax.driver.mapping.MappingManager(session));
  }
}
