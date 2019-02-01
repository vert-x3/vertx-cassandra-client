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
import java.util.UUID;

/**
 * Eclipse Vert.x Cassandra client.
 *
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@VertxGen
public interface CassandraClient {

  /**
   * The default shared client name.
   */
  String DEFAULT_SHARED_CLIENT_NAME = "DEFAULT";

  /**
   * Like {@link CassandraClient#createNonShared(Vertx, CassandraClientOptions)} with default options.
   */
  static CassandraClient createNonShared(Vertx vertx) {
    return createNonShared(vertx, new CassandraClientOptions());
  }

  /**
   * Create a Cassandra client which maintains its own driver session.
   * <p>
   * It is not recommended to create several non shared clients in an application.
   *
   * @param vertx the Vert.x instance
   * @param options the options
   *
   * @return the client
   */
  static CassandraClient createNonShared(Vertx vertx, CassandraClientOptions options) {
    return new CassandraClientImpl(vertx, UUID.randomUUID().toString(), options);
  }

  /**
   * Like {@link CassandraClient#createShared(Vertx, String, CassandraClientOptions)} with default options and client name.
   */
  static CassandraClient createShared(Vertx vertx) {
    return createShared(vertx, DEFAULT_SHARED_CLIENT_NAME);
  }

  /**
   * Like {@link CassandraClient#createShared(Vertx, String, CassandraClientOptions)} with default options.
   */
  static CassandraClient createShared(Vertx vertx, String clientName) {
    return createShared(vertx, clientName, new CassandraClientOptions());
  }

  /**
   * Like {@link CassandraClient#createShared(Vertx, String, CassandraClientOptions)} with default client name.
   */
  static CassandraClient createShared(Vertx vertx, CassandraClientOptions options) {
    return createShared(vertx, DEFAULT_SHARED_CLIENT_NAME, options);
  }

  /**
   * Create a Cassandra client that shares its driver session with any other client having the same name.
   *
   * @param vertx the Vert.x instance
   * @param options the options
   * @param clientName the shared client name
   *
   * @return the client
   */
  static CassandraClient createShared(Vertx vertx, String clientName, CassandraClientOptions options) {
    return new CassandraClientImpl(vertx, clientName, options);
  }

  /**
   * @return whether this Cassandra client instance is connected
   */
  boolean isConnected();

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is fully fetched.
   * @param query the query to execute
   *
   * @return current Cassandra client instance
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  CassandraClient executeWithFullFetch(String query, Handler<AsyncResult<List<Row>>> resultHandler);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is fully fetched.
   * @param statement the statement to execute
   *
   * @return current Cassandra client instance
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  CassandraClient executeWithFullFetch(Statement statement, Handler<AsyncResult<List<Row>>> resultHandler);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is present, but can be not fully fetched
   * @param query the query to execute
   *
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient execute(String query, Handler<AsyncResult<ResultSet>> resultHandler);

  /**
   * Execute the statement and provide a handler for consuming results.
   *
   * @param resultHandler handler called when result of execution is present
   * @param statement the statement to execute
   *
   * @return current Cassandra client instance
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  CassandraClient execute(Statement statement, Handler<AsyncResult<ResultSet>> resultHandler);

  /**
   * Prepares the provided query string.
   *
   * @param resultHandler handler called when result of query preparation is present
   * @param query the query to prepare
   *
   * @return current Cassandra client instance
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  CassandraClient prepare(String query, Handler<AsyncResult<PreparedStatement>> resultHandler);

  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query as a read stream.
   *
   * @param sql the SQL to execute. For example <code>SELECT * FROM table ...</code>.
   * @param rowStreamHandler the handler which is called once the operation completes. It will return an instance of {@link CassandraRowStream}.
   *
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient queryStream(String sql, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler);

  /**
   * Executes the given SQL statement which returns the results of the query as a read stream.
   *
   * @param statement the statement to execute.
   * @param rowStreamHandler the handler which is called once the operation completes. It will return an instance of {@link CassandraRowStream}.
   *
   * @return current Cassandra client instance
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Fluent
  CassandraClient queryStream(Statement statement, Handler<AsyncResult<CassandraRowStream>> rowStreamHandler);

  /**
   * Closes this client.
   *
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient close();

  /**
   * Closes this client.
   *
   * @param closeHandler handler called when client is closed
   *
   * @return current Cassandra client instance
   */
  @Fluent
  CassandraClient close(Handler<AsyncResult<Void>> closeHandler);
}
