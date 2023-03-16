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

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import io.vertx.cassandra.impl.CassandraClientImpl;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collector;

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
   * Like {@link CassandraClient#create(Vertx, CassandraClientOptions)} with default options.
   */
  static CassandraClient create(Vertx vertx) {
    return create(vertx, new CassandraClientOptions());
  }

  /**
   * Create a Cassandra client which maintains its own driver session.
   * <p>
   * It is not recommended to create several non shared clients in an application.
   *
   * @param vertx   the Vert.x instance
   * @param options the options
   * @return the client
   */
  static CassandraClient create(Vertx vertx, CassandraClientOptions options) {
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
   * @param vertx      the Vert.x instance
   * @param options    the options
   * @param clientName the shared client name
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
   * @param query         the query to execute
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<Row>> executeWithFullFetch(String query);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param statement     the statement to execute
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<Row>> executeWithFullFetch(Statement statement);

  /**
   * Execute the query and provide a handler for consuming results.
   *
   * @param query         the query to execute
   * @return a future of the result
   */
  Future<ResultSet> execute(String query);

  /**
   * Execute a query and produce a result by applying a collector to result set rows.
   *
   * @param query         the query to execute
   * @param collector     the collector to use to produce a result
   * @param <R>           the result type
   * @return a future of the result
   */
  @GenIgnore
  <R> Future<R> execute(String query, Collector<Row, ?, R> collector);

  /**
   * Execute the statement and provide a handler for consuming results.
   *
   * @param statement     the statement to execute
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<ResultSet> execute(Statement statement);

  /**
   * Execute a statement and produce a result by applying a collector to result set rows.
   *
   * @param statement     the statement to execute
   * @param collector     the collector to use to produce a result
   * @param <R>           the result type
   * @return a future of the result
   */
  @GenIgnore
  <R> Future<R> execute(Statement statement, Collector<Row, ?, R> collector);

  /**
   * Prepares the provided query string.
   *
   * @param query         the query to prepare
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<PreparedStatement> prepare(String query);

  /**
   * Prepares the provided a {@link SimpleStatement}.
   *
   * @param statement     the statement to prepare
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<PreparedStatement> prepare(SimpleStatement statement);

  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query as a read stream.
   *
   * @param sql              the SQL to execute. For example <code>SELECT * FROM table ...</code>.
   * @return a future of the result
   */
  Future<CassandraRowStream> queryStream(String sql);

  /**
   * Executes the given SQL statement which returns the results of the query as a read stream.
   *
   * @param statement        the statement to execute.
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<CassandraRowStream> queryStream(Statement statement);

  /**
   * Closes this client.
   *
   * @return a future of the result
   */
  Future<Void> close();

  /**
   * Get {@link Metadata} for the session.
   *
   * @return a future of the result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<Metadata> metadata();
}
