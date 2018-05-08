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

import io.vertx.core.Vertx;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * A base class, which should be used for in all test where Cassandra service is required.
 */
public class CassandraServiceBase {

  Vertx vertx = Vertx.vertx();
  static final String HOST = "localhost";
  static final int NATIVE_TRANSPORT_PORT = 9142;

  @Before
  public void before() throws InterruptedException, IOException, TTransportException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();

  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}
