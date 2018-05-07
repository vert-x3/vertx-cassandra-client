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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.Test;

public class ExampleTest extends CassandraServiceBase {
  @Test
  public void simple() {
    Cluster cluster = null;
    try {
      cluster = Cluster.builder()                                                    // (1)
        .addContactPoint(HOST)
        .withPort(NATIVE_TRANSPORT_PORT)
        .build();
      Session session = cluster.connect();                                           // (2)

      ResultSet rs = session.execute("select release_version from system.local");    // (3)
      Row row = rs.one();
      System.out.println(row.getString("release_version"));                          // (4)
    } finally {
      if (cluster != null) cluster.close();                                          // (5)
    }
  }
}
