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
import com.datastax.driver.mapping.MappingManager;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.VertxMapper;
import io.vertx.cassandra.VertxMappingManager;
import io.vertx.core.Vertx;

/**
 * @author Martijn Zwennes
 */
public class VertxMappingManagerImpl implements VertxMappingManager {

  private final MappingManager mappingManager;

  public VertxMappingManagerImpl(CassandraClient client) {
    CassandraClientImpl clientImpl =  (CassandraClientImpl) client;
    this.mappingManager = new MappingManager(clientImpl.getSession());
  }

  @Override
  public MappingManager getMappingManager() {
    return mappingManager;
  }

  @Override
  public <T> VertxMapper<T> mapper(Class<T> clazz, Vertx vertx) {
    Mapper<T> mapper = getMappingManager().mapper(clazz);
    return new VertxMapperImpl<>(mapper, vertx);
  }

}
