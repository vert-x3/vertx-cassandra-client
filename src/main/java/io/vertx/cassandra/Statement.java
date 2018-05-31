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
import io.vertx.cassandra.impl.StatementImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;

/**
 * Analogy of {@link com.datastax.driver.core.Statement}
 * <p>
 * Represents a query, which is ready to be executed via {@link CassandraClient#execute(Statement, Handler)}
 */
@VertxGen
public interface Statement {

  /**
   * Construct a new instance of {@link Statement} from the string.
   *
   * @param query the string
   * @return a new instance of {@link Statement}
   * @see SimpleStatement
   */
  static Statement fromString(String query) {
    return new StatementImpl(new SimpleStatement(query));
  }

  /**
   * Sets the consistency level for the query
   *
   * @param consistency the consistency level to set
   * @return current {@link Statement} instance
   */
  @Fluent
  Statement setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * The consistency level for this query
   *
   * @return the consistency level for this query, or {@code null} if no
   * consistency level has been specified (through {@link Statement#setConsistencyLevel(ConsistencyLevel)}).
   * In the latter case, the default consistency level will be used.
   */
  ConsistencyLevel getConsistencyLevel();
}
