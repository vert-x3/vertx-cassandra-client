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

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

/**
 * Analogy of {@link com.datastax.driver.core.PreparedStatement}
 *
 * @see com.datastax.driver.core.PreparedStatement
 */
@VertxGen
public interface PreparedQuery {

  /**
   * Creates a new {@link ExecutableQuery} object and bind its variables to the  provided values.
   *
   * @param params the values to bind
   * @return instance of {@link ExecutableQuery} which can be executed with {@link CassandraClient#execute(ExecutableQuery, Handler)}
   * @see com.datastax.driver.core.PreparedStatement#bind(Object...)
   */
  ExecutableQuery bind(JsonArray params);
}
