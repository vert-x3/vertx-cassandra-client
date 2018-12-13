/*
 * Copyright 2019 The Vert.x Community.
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

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(VertxUnitRunner.class)
public class MapperTest extends CassandraServiceBase {

  @Test
  public void mappingManagerInitialize(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );

    Async async = context.async();
    cassandraClient.connect(connectEvent -> {
      if (connectEvent.succeeded()) {
        VertxMappingManager manager = VertxMappingManager.create(cassandraClient);
        context.assertNotNull(manager);
        async.complete();
      }
    });
  }

  @Test
  public void initializeMapper(TestContext context) {
    CassandraClient cassandraClient = CassandraClient.createNonShared(
      vertx,
      new CassandraClientOptions().setPort(NATIVE_TRANSPORT_PORT)
    );

    TestExample test = new TestExample("foo", 1);

    // set handler for getting object using mapper
    Async async = context.async();
    Future<TestExample> getHandler = Future.future();
    getHandler.setHandler(ar -> {
      if (ar.succeeded()) {
        context.assertEquals(test.name, ar.result().name);
        context.assertEquals(test.age, ar.result().age);
        async.complete();
      } else {
        context.fail("could not retrieve object from table");
      }
    });

    // connect to Cassandra cluster and save/get an object
    cassandraClient.connect(connectEvent -> {
      if (connectEvent.succeeded()) {
        VertxMappingManager manager = VertxMappingManager.create(cassandraClient);
        VertxMapper<TestExample> mapper = manager.mapper(TestExample.class, vertx);
        mapper.save(test, ar -> {
          if (ar.succeeded()) {
            mapper.get(Collections.singletonList("foo"), getHandler);
          } else {
            context.fail("could not save object to table");
          }
        });
      } else {
        context.fail("could not access cassandra database");
      }
    });

  }

  @Table(name = "mapper_test", keyspace = "names")
  private static class TestExample {

    @PartitionKey private String name;
    private int age;

    TestExample(String name, int age) {
      this.name = name;
      this.age = age;
    }

    TestExample() {}
  }
}
