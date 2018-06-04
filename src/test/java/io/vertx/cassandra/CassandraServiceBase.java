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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

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

    CassandraClient cassandraClient = CassandraClient.create(
      vertx,
      new CassandraClientOptions()
        .addContactPoint(HOST)
        .setPort(NATIVE_TRANSPORT_PORT)
    );
    Future<Void> future = Future.future();
    CountDownLatch latch = new CountDownLatch(1);
    cassandraClient.connect(future);
    Future<Void> result = future.compose(connected -> {
      Future<ResultSet> createKeySpace = Future.future();
      cassandraClient.execute("CREATE KEYSPACE IF NOT EXISTS names WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", createKeySpace);
      return createKeySpace;
    }).compose(keySpaceCreated -> {
      Future<ResultSet> createTable = Future.future();
      cassandraClient.execute("create table names.names_by_first_letter (first_letter text, name text, primary key (first_letter, name));", createTable);
      return createTable;
    }).compose(tableCreated -> {
      Future<Void> disconnectFuture = Future.future();
      cassandraClient.disconnect(disconnectFuture);
      return disconnectFuture;
    }).setHandler(handler -> {
      if (handler.failed()) {
        Assert.fail();
      } else {
        latch.countDown();
      }
    });

    latch.await();
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}
