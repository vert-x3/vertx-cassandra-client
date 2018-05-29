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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import io.vertx.cassandra.impl.ExecutableQueryImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;

@VertxGen
public interface ExecutableQuery {

  static ExecutableQuery fromString(String query) {
    return new ExecutableQueryImpl(new SimpleStatement(query));
  }

  @Fluent
  ExecutableQuery setConsistencyLevel(ConsistencyLevel consistency);

  ConsistencyLevel getConsistencyLevel();
}
