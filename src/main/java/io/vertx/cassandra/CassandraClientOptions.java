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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Eclipse Vert.x Cassandra client options.
 *
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
@DataObject(generateConverter = true)
public class CassandraClientOptions {

  /**
   * Default port for connecting with Cassandra service.
   */
  public static final int DEFAULT_PORT = 9042;

  /**
   * Default host for connecting with Cassandra service.
   */
  public static final String DEFAULT_HOST = "localhost";

  private CqlSessionBuilder builder;
  private String keyspace;

  /**
   * Default constructor.
   */
  public CassandraClientOptions() {
    this(CqlSession.builder());
  }

  /**
   * Constructor using an existing {@link CqlSessionBuilder} instance.
   */
  public CassandraClientOptions(CqlSessionBuilder builder) {
    this.builder = builder;
  }

  /**
   * Constructor to create options from JSON.
   *
   * @param json the JSON
   */
  public CassandraClientOptions(JsonObject json) {
    this();
    CassandraClientOptionsConverter.fromJson(json, this);
  }

  /**
   * @return a JSON representation of these options
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CassandraClientOptionsConverter.toJson(this, json);
    return json;
  }

  /**
   * Adds a contact point to use for the initial connection to the cluster
   *
   * @param address the address
   * @return a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions addContactPoint(InetSocketAddress address) {
    builder.addContactPoint(address);
    return this;
  }

  /**
   * Adds a contact point to use for the initial connection to the cluster
   *
   * @param host the address
   * @param port the port
   * @return a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions addContactPoint(String host, int port) {
    builder.addContactPoint(InetSocketAddress.createUnresolved(host, port));
    return this;
  }

  /**
   * @return a cluster builder, which will be used by the client
   */
  public CqlSessionBuilder dataStaxClusterBuilder() {
    return builder;
  }

  /**
   * @return the keyspace to use when creating the Cassandra session
   */
  public String getKeyspace() {
    return keyspace;
  }

  /**
   * Set the keyspace to use when creating the Cassandra session. Defaults to {@code null}.
   *
   * @param keyspace the keyspace to use when creating the Cassandra session
   *
   * @return a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions setKeyspace(String keyspace) {
    this.keyspace = keyspace;
    builder.withKeyspace(keyspace);
    return this;
  }
}
