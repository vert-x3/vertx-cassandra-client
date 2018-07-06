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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import io.vertx.cassandra.impl.CassandraClientImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.List;

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
  CassandraClient connect();

  /**
   * Connect to a Cassandra service.
   *
   * @param connectHandler handler called when asynchronous connect call ends
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient connect(Handler<AsyncResult<Void>> connectHandler);

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
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is fully fetched.
   * @param query         the query to execute
   * @return current Cassandra client instance
   */
  @GenIgnore
  @Fluent
  CassandraClient executeWithFullFetch(String query, Handler<AsyncResult<List<Row>>> resultHandler);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is fully fetched.
   * @param statement     the statement to execute
   * @return current Cassandra client instance
   */
  @GenIgnore
  @Fluent
  CassandraClient executeWithFullFetch(Statement statement, Handler<AsyncResult<List<Row>>> resultHandler);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is present, but can be not fully feched
   * @param query         the query to execute
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler);
  
  /**
   * Execute the statement and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is present
   * @param statement         the statement to execute
   * @return current Cassandra client instance
   */
  @GenIgnore
  CassandraClient execute(Statement statement, Handler<AsyncResult<ResultSet>> resultHandler);

  /**
   * Prepares the provided query string.
   *
   * @param resultHandler handler called when result of query preparation is present
   * @param query         the query to prepare
   * @return current Cassandra client instance
   */
  @GenIgnore
  CassandraClient prepare(String query, Handler<AsyncResult<PreparedStatement>> resultHandler);


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
   * Executes the given SQL statement which returns the results of the query as a read stream.
   *
   * @param statement        the statement to execute.
   * @param rowStreamHandler the handler which is called once the operation completes. It will return an instance of {@link CassandraRowStream}.
   * @return current Cassandra client instance
   */
  @GenIgnore
  @Fluent
  CassandraClient queryStream(Statement statement, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler);

  /**
   * Disconnects from the Cassandra service.
   *
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient disconnect();

  /**
   * Disconnects from the Cassandra service.
   *
   * @param disconnectHandler handler called when asynchronous disconnect call ends
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient disconnect(Handler<AsyncResult<Void>> disconnectHandler);
}
