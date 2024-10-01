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
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(VertxUnitRunner.class)
public class CloseHookTest extends CassandraClientTestBase {

  private CassandraClient shared;

  @After
  public void closeSharedClient(TestContext testContext) {
    if (shared != null) {
      shared.close().onComplete(testContext.asyncAssertSuccess());
    }
  }

  @Test
  public void testClientClosedAfterUndeploy(TestContext testContext) {
    String clientName = randomClientName();
    VerticleWithCassandraClient verticle = new VerticleWithCassandraClient(createClientOptions(), clientName, true, false);
    vertx.deployVerticle(verticle).onComplete(testContext.asyncAssertSuccess(id -> {
      vertx.undeploy(id).onComplete(testContext.asyncAssertSuccess(v -> {
        CassandraClient client = CassandraClient.createShared(vertx, clientName, createClientOptions());
        testContext.assertFalse(client.isConnected());
      }));
    }));
  }

  @Test
  public void testExternalSharedClientNotClosedAfterUndeploy(TestContext testContext) {
    String clientName = randomClientName();
    shared = CassandraClient.createShared(vertx, clientName, createClientOptions());
    assertFalse(shared.isConnected());
    VerticleWithCassandraClient verticle = new VerticleWithCassandraClient(createClientOptions(), clientName, true, true);
    vertx.deployVerticle(verticle).onComplete(testContext.asyncAssertSuccess(id -> {
      testContext.assertTrue(shared.isConnected());
      vertx.undeploy(id).onComplete(testContext.asyncAssertSuccess(v -> {
        testContext.assertTrue(shared.isConnected());
      }));
    }));
  }

}
