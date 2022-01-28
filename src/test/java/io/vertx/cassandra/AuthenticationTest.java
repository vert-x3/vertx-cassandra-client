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
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(VertxUnitRunner.class)
public class AuthenticationTest {

  private static final int CQL_PORT = 9042;

  @Rule
  public RunTestOnContext vertx = new RunTestOnContext();

  @Rule
  public GenericContainer<?> authContainer = new GenericContainer<>(
    new ImageFromDockerfile()
      .withFileFromClasspath("Dockerfile", "auth-container/Dockerfile")
  )
    .waitingFor(Wait.forLogMessage(".*Created default superuser role 'cassandra'.*\\n", 1))
    .withExposedPorts(CQL_PORT);

  private CassandraClient client;

  @BeforeClass
  public static void notMacOs(){
    Assume.assumeFalse("Test does not work on a Mac OS", io.netty.util.internal.PlatformDependent.isOsx());
  }

  @Test
  public void testAuthProvider(TestContext context) {
    CassandraClientOptions options = new CassandraClientOptions()
      .addContactPoint("localhost", authContainer.getMappedPort(CQL_PORT))
      .setUsername("cassandra")
      .setPassword("cassandra");
    options.dataStaxClusterBuilder().withLocalDatacenter("datacenter1");
    client = CassandraClient.createShared(vertx.vertx(), options);
    client.executeWithFullFetch("select release_version from system.local", context.asyncAssertSuccess(result -> {
      context.verify(unused -> {
        assertThat(result.iterator().next().getString("release_version"), startsWith("3.11"));
      });
    }));
  }

  @After
  public void tearDown(TestContext context) {
    if (client != null) {
      client.close(context.asyncAssertSuccess());
    }
  }
}
