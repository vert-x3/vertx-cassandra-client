/*
 * Copyright 2019 Red Hat, Inc.
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

package io.vertx.cassandra;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(VertxUnitRunner.class)
public class CloseHookTest extends CassandraClientTestBase {

  private static final String TEST_CLIENT_NAME = "TEST_CLIENT";

  private CassandraClient shared;

  @After
  public void closeSharedClient(TestContext testContext) {
    if (shared != null) {
      shared.close(testContext.asyncAssertSuccess());
    }
  }

  @Test
  public void testClientClosedAfterUndeploy(TestContext testContext) {
    VerticleWithCassandraClient verticle = new VerticleWithCassandraClient(createClientOptions(), false);
    vertx.deployVerticle(verticle, testContext.asyncAssertSuccess(id -> {
      vertx.undeploy(id, testContext.asyncAssertSuccess(v -> {
        CassandraClient client = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, createClientOptions());
        testContext.assertFalse(client.isConnected());
      }));
    }));
  }

  @Test
  public void testExternalSharedClientNotClosedAfterUndeploy(TestContext testContext) {
    shared = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, createClientOptions());
    assertFalse(shared.isConnected());
    VerticleWithCassandraClient verticle = new VerticleWithCassandraClient(createClientOptions(), true);
    vertx.deployVerticle(verticle, testContext.asyncAssertSuccess(id -> {
      testContext.assertTrue(shared.isConnected());
      vertx.undeploy(id, testContext.asyncAssertSuccess(v -> {
        testContext.assertTrue(shared.isConnected());
      }));
    }));
  }

  private static class VerticleWithCassandraClient extends AbstractVerticle {

    final CassandraClientOptions options;
    final boolean closeClientOnStop;
    CassandraClient client;

    VerticleWithCassandraClient(CassandraClientOptions options, boolean closeManuallyOnStop) {
      this.options = options;
      this.closeClientOnStop = closeManuallyOnStop;
    }

    @Override
    public void start(Future<Void> startFuture) {
      client = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, options);
      getCassandraReleaseVersion(client, ar -> {
        if (ar.succeeded()) {
          startFuture.complete();
        } else {
          startFuture.fail(ar.cause());
        }
      });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
      if (closeClientOnStop && client != null) {
        client.close(stopFuture);
      } else {
        stopFuture.complete();
      }
    }
  }
}
