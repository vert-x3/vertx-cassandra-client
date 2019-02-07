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

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(VertxUnitRunner.class)
public class ConnectionTest extends CassandraClientTestBase {

  private static final String IP_HOST_WITHOUT_CASSANDRA = "100.100.100.100";

  @Test
  public void testDefaultHost(TestContext testContext) {
    getCassandraReleaseVersion(client, testContext.asyncAssertSuccess(version -> {
      testContext.assertTrue(version.matches("\\d+\\.\\d+\\.\\d"));
    }));
  }

  @Test
  public void testFailIfNoHostAvailable(TestContext testContext) {
    client.close();
    CassandraClientOptions options = createClientOptions()
      .setContactPoints(Collections.singletonList(IP_HOST_WITHOUT_CASSANDRA));
    client = CassandraClient.createNonShared(vertx, options);
    client.executeWithFullFetch("select release_version from system.local", testContext.asyncAssertFailure());
  }
}

