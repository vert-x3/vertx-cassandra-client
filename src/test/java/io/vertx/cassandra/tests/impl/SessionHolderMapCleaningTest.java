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
import io.vertx.core.*;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class SessionHolderMapCleaningTest {

  private static final String CLIENT_NAME = "test";

  private Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after() throws Exception {
    vertx.close().await(20, TimeUnit.SECONDS);
  }

  @Test
  public void testMapCleaned() throws Exception {
    LocalMap<String, SessionHolder> holders = vertx.sharedData().getLocalMap(CassandraClientImpl.HOLDERS_LOCAL_MAP_NAME);
    int instances = 5;
    String id = vertx.deployVerticle(SampleVerticle::new, new DeploymentOptions().setInstances(instances))
      .await(20, TimeUnit.SECONDS);
    assertEquals(instances, holders.get(CLIENT_NAME).refCount());
    vertx.undeploy(id).await(20, TimeUnit.SECONDS);
    assertEquals(0, holders.size());
  }

  private static class SampleVerticle extends VerticleBase {

    CassandraClient shared;

    @Override
    public Future<?> start() throws Exception {
      shared = CassandraClient.createShared(vertx, CLIENT_NAME);
      return super.start();
    }
  }
}
