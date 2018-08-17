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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
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
   * Default port for connecting with Cassandra service.
   */
  public static final String DEFAULT_HOST = "localhost";

  private List<String> contactPoints = new ArrayList<>();

  private int port = DEFAULT_PORT;

  /**
   * Default constructor.
   */
  public CassandraClientOptions() {
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
   * Set a list of hosts, where some of cluster nodes is located.
   *
   * @param contactPoints the list of hosts
   * @return a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions setContactPoints(List<String> contactPoints) {
    this.contactPoints = contactPoints;
    return this;
  }

  /**
   * Set which port should be used for all the hosts to connect to a cassandra service.
   *
   * @param port the port
   * @return a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Add a address, where a cluster node is located.
   * @param address the address
   * @return  a reference to this, so the API can be used fluently
   */
  public CassandraClientOptions addContactPoint(String address) {
    contactPoints().add(address);
    return this;
  }

  /**
   * @return list of address used by the client for connecting with a cassandra service
   */
  public List<String> contactPoints() {
    if (contactPoints == null) {
      contactPoints = new ArrayList<>();
    }
    return contactPoints;
  }

  /**
   * @return port, used for connecting with a cassandra service
   */
  public int port() {
    return port;
  }
}
