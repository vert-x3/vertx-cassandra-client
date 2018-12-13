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

import com.datastax.driver.mapping.MappingManager;
import io.vertx.cassandra.impl.VertxMappingManagerImpl;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;

/**
 * A Vert.x wrapped datastax MappingManager {@link com.datastax.driver.mapping.MappingManager}.
 *
 * @author Martijn Zwennes
 */
@VertxGen
public interface VertxMappingManager {

  /**
   * Create a VertxMappingManager from the given CassandraClient.
   *
   * @param client a Cassandra client instance
   * @return a VertxMappingManager instance which is a wrapper for the datastax MappingManager
   */
  static VertxMappingManager create(CassandraClient client) {
    return new VertxMappingManagerImpl(client);
  }

  /**
   * @return the underlying datastax MappingManager.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  MappingManager getMappingManager();

  /**
   * Returns a vert.x wrapped mapper which allows conversion
   * of domain classes to and from query results.
   *
   * Example usage:
   *
   * {@literal @}Table(keyspace = "test", name = "users")
   * class User {
   *   {@literal @}PartitionKey String name;
   *   ...
   * }
   *
   * VertxMappingManager manager = cassandraClient.getMappingManager();
   * VertxMapper<User> mapper = manager.mapper(User.class, vertx);
   *
   * mapper.save(new User("john", resultHandler));
   *
   * @param clazz class definition (e.g. User.class)
   * @param vertx vertx instance
   * @return a vert.x wrapped mapper for the given class
   */
  <T> VertxMapper<T> mapper(Class<T> clazz, Vertx vertx);
}
