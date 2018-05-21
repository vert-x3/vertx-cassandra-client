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

import io.vertx.cassandra.impl.CassandraClientImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Eclipse Vert.x Cassandra client.
 */
@VertxGen
public interface CassandraClient {

  static CassandraClient create(Vertx vertx) {
    return create(vertx, new CassandraClientOptions());
  }

  static CassandraClient create(Vertx vertx, CassandraClientOptions cassandraClientOptions) {
    return new CassandraClientImpl(vertx, cassandraClientOptions);
  }

  /**
   * Connect to a Cassandra service.
   *
   * @return current Cassandra client instance
   */
  @Fluent
  default CassandraClient connect() {
    return connect(null);
  }

  /**
   * Connect to a Cassandra service.
   *
   * @param connectHandler handler called when asynchronous connect call ends
   * @return current Cassandra client instance
   */
  @Fluent
  default CassandraClient connect(Handler<AsyncResult<Void>> connectHandler) {
    return connect(null, connectHandler);
  }

  /**
   * Connect to a Cassandra service.
   *
   * @param keyspace       The name of the keyspace to use for the created connection.
   * @param connectHandler handler called when asynchronous connect call ends
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient connect(String keyspace, Handler<AsyncResult<Void>> connectHandler);


  /**
   * Execute the query and provide a handler for consuming results
   *
   * @param resultHandler handler called when result of execution is present
   * @param query         the query to execute
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler);


  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query as a read stream.
   *
   * @param sql              the SQL to execute. For example <code>SELECT * FROM table ...</code>.
   * @param rowStreamHandler the handler which is called once the operation completes. It will return an instance of {@link CassandraRowStream}.
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient queryStream(String sql, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler);

  /**
   * Disconnects from the Cassandra service.
   *
   * @return current Cassandra client instance
   */
  @Fluent
  default CassandraClient disconnect() {
    return disconnect(null);
  }

  /**
   * Disconnects from the Cassandra service.
   *
   * @param disconnectHandler handler called when asynchronous disconnect call ends
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient disconnect(Handler<AsyncResult<Void>> disconnectHandler);
}
