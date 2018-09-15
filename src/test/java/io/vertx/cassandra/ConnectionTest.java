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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class ConnectionTest extends CassandraServiceBase {

  private static final Logger log = LoggerFactory.getLogger(ConnectionTest.class);

  @Test
  public void connectDisconnectTest(TestContext context) {
    CassandraClientOptions options = new CassandraClientOptions()
      .addContactPoint(HOST)
      .setPort(NATIVE_TRANSPORT_PORT);
    connectAndDisconnect(context, options);
  }

  @Test
  public void defaultContactPointShouldWorks(TestContext context) {
    CassandraClientOptions options = new CassandraClientOptions()
      .setPort(NATIVE_TRANSPORT_PORT);
    connectAndDisconnect(context, options);
  }

  private void connectAndDisconnect(TestContext context, CassandraClientOptions options) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      options
    );
    Async async = context.async(2);
    cassandraClient.connect("system", connectEvent -> {
      checkContext(context);
      if (connectEvent.succeeded()) {
        log.info("successfully connected");
        async.countDown();
        cassandraClient.disconnect(disconnectEvent -> {
          checkContext(context);
          if (disconnectEvent.succeeded()) {
            log.info("successfully disconnected");
            async.countDown();
          } else {
            context.fail();
          }
        });
      } else {
        context.fail();
      }
    });
  }
}
