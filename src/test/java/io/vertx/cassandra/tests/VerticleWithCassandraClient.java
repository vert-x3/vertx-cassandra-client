/*
 * Copyright 2021 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.cassandra.tests;

import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.core.Future;
import io.vertx.core.VerticleBase;

public class VerticleWithCassandraClient extends VerticleBase {

  private final CassandraClientOptions options;
  private final String clientName;
  private final boolean executeRequestOnStart;
  private final boolean closeOnStop;

  private CassandraClient client;

  public VerticleWithCassandraClient(CassandraClientOptions options, String clientName, boolean executeRequestOnStart, boolean closeOnStop) {
    this.options = options;
    this.clientName = clientName;
    this.executeRequestOnStart = executeRequestOnStart;
    this.closeOnStop = closeOnStop;
  }

  @Override
  public Future<?> start() throws Exception {
    client = CassandraClient.createShared(vertx, clientName, options);
    if (executeRequestOnStart) {
      return CassandraClientTestBase.getCassandraReleaseVersion(client);
    } else {
      return super.start();
    }
  }

  @Override
  public Future<?> stop() throws Exception {
    if (closeOnStop && client != null) {
      return client.close();
    } else {
      return super.stop();
    }
  }
}
