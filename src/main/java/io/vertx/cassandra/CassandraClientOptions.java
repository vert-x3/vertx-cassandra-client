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
 */
@DataObject(generateConverter = true)
public class CassandraClientOptions {
  
  public static final int DEFAULT_PORT = 9042;

  public static final String DEFAULT_HOST = "localhost";

  private List<String> contactPoints = new ArrayList<>();

  private int port = DEFAULT_PORT;

  public CassandraClientOptions() {
  }

  public CassandraClientOptions(JsonObject json) {
    this();
    CassandraClientOptionsConverter.fromJson(json, this);
  }

  public CassandraClientOptions setContactPoints(List<String> contactPoints) {
    this.contactPoints = contactPoints;
    return this;
  }

  public CassandraClientOptions setPort(int port) {
    this.port = port;
    return this;
  }

  public CassandraClientOptions addContactPoint(String address) {
    contactPoints().add(address);
    return this;
  }

  public List<String> contactPoints() {
    if (contactPoints == null) {
      contactPoints = new ArrayList<>();
    }
    return contactPoints;
  }

  public int port() {
    return port;
  }
}
