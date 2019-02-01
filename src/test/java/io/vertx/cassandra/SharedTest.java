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

import java.util.regex.Pattern;

@RunWith(VertxUnitRunner.class)
public class SharedTest extends CassandraClientTestBase {

  public static final String TEST_CLIENT_NAME = "TEST_CLIENT";

  private CassandraClient shared;

  @After
  public void closeShared(TestContext testContext) {
    if (shared != null) {
      shared.close(testContext.asyncAssertSuccess());
    }
  }

  @Test
  public void testSharedClientNotClosed(TestContext testContext) {
    client = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, createClientOptions());
    client.executeWithFullFetch("select release_version from system.local", testContext.asyncAssertSuccess(rows -> {
      String release_version = rows.iterator().next().getString("release_version");
      testContext.assertTrue(Pattern.compile("[0-9\\.]+").matcher(release_version).find());
      vertx.deployVerticle(new SampleVerticle(createClientOptions(), false), testContext.asyncAssertSuccess(id -> {
        vertx.undeploy(id, testContext.asyncAssertSuccess(v2 -> {
          testContext.assertTrue(client.isConnected());
        }));
      }));
    }));
  }

  private static class SampleVerticle extends AbstractVerticle {

    final CassandraClientOptions options;
    final boolean closeOnStop;
    CassandraClient client;

    SampleVerticle(CassandraClientOptions options, boolean closeOnStop) {
      this.options = options;
      this.closeOnStop = closeOnStop;
    }

    @Override
    public void start() {
      client = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, options);
    }

    @Override
    public void stop(Future<Void> stopFuture) {
      if (closeOnStop) {
        client.close(stopFuture);
      } else {
        stopFuture.complete();
      }
    }
  }
}
