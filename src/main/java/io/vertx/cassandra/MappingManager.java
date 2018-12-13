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

import io.vertx.cassandra.impl.MappingManagerImpl;
import io.vertx.codegen.annotations.VertxGen;

/**
 * It is like {@link com.datastax.driver.mapping.MappingManager}, but adapted for Vert.x.
 *
 * @author Martijn Zwennes
 */
@VertxGen
public interface MappingManager {

  /**
   * Create a {@link MappingManager} from the given {@link CassandraClient}.
   *
   * @param client a Cassandra client instance
   */
  static MappingManager create(CassandraClient client) {
    return new MappingManagerImpl(client);
  }

  /**
   * Create a {@link Mapper} that allows conversion of domain classes to and from query results.
   *
   * @param mappedClass mapped class
   */
  <T> Mapper<T> mapper(Class<T> mappedClass);
}
