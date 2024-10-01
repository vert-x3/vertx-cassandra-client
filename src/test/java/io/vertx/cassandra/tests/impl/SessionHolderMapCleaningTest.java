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

package io.vertx.cassandra.tests.impl;

import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.impl.CassandraClientImpl;
import io.vertx.cassandra.impl.SessionHolder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

public class SessionHolderMapCleaningTest extends VertxTestBase {

  private static final String CLIENT_NAME = "test";

  @Test
  public void testMapCleaned() {
    LocalMap<String, SessionHolder> holders = vertx.sharedData().getLocalMap(CassandraClientImpl.HOLDERS_LOCAL_MAP_NAME);
    int instances = 5;
    vertx.deployVerticle(() -> new SampleVerticle(), new DeploymentOptions().setInstances(instances)).onComplete(onSuccess(id -> {
      assertEquals(instances, holders.get(CLIENT_NAME).refCount());
      vertx.undeploy(id).onComplete(onSuccess(v -> {
        assertEquals(0, holders.size());
        testComplete();
      }));
    }));
    await();
  }

  private static class SampleVerticle extends AbstractVerticle {

    CassandraClient shared;

    @Override
    public void start() throws Exception {
      shared = CassandraClient.createShared(vertx, CLIENT_NAME);
    }
  }
}
