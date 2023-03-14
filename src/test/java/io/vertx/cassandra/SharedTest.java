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

package io.vertx.cassandra;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

@RunWith(VertxUnitRunner.class)
public class SharedTest extends CassandraClientTestBase {

  private CassandraClient shared;

  @After
  public void closeShared(TestContext testContext) {
    if (shared != null) {
      shared.close().onComplete(testContext.asyncAssertSuccess());
    }
  }

  @Test
  public void testSharedClientNotClosed(TestContext testContext) {
    String clientName = randomClientName();
    client = CassandraClient.createShared(vertx, clientName, createClientOptions());
    client.executeWithFullFetch("select release_version from system.local").onComplete(testContext.asyncAssertSuccess(rows -> {
      String release_version = rows.iterator().next().getString("release_version");
      testContext.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
      vertx.deployVerticle(new VerticleWithCassandraClient(createClientOptions(), clientName, false, true))
        .onComplete(testContext.asyncAssertSuccess(id -> {
        vertx.undeploy(id).onComplete(testContext.asyncAssertSuccess(v2 -> {
          testContext.assertTrue(client.isConnected());
        }));
      }));
    }));
  }
}
