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
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.cassandraunit.dataset.CQLDataSet;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

/**
 * @author Martijn Zwennes
 */
@RunWith(VertxUnitRunner.class)
public class MapperTest extends CassandraClientTestBase {

  @Test
  public void testSaveAndGet(TestContext context) {
    cqlDataLoader.load(new MapperDataSet());
    MappingManager manager = MappingManager.create(client);
    Mapper<Mapped> mapper = manager.mapper(Mapped.class);
    Mapped expected = new Mapped("foo", 1);
    mapper.save(expected, context.asyncAssertSuccess(saved -> {
      List<Object> primaryKey = Collections.singletonList("foo");
      mapper.get(primaryKey, context.asyncAssertSuccess(actual -> {
        context.assertEquals(expected.name, actual.name);
        context.assertEquals(expected.age, actual.age);
        mapper.delete(primaryKey, context.asyncAssertSuccess(deleted -> {
          mapper.get(primaryKey, context.asyncAssertSuccess(res -> {
            context.assertNull(res);
          }));
        }));
      }));
    }));
  }

  @Table(name = "mapper_test", keyspace = "mapper")
  private static class Mapped {

    @PartitionKey
    private String name;
    private int age;

    Mapped(String name, int age) {
      this.name = name;
      this.age = age;
    }

    Mapped() {}
  }

  private static class MapperDataSet implements CQLDataSet {
    @Override
    public List<String> getCQLStatements() {
      return Collections.singletonList("create table mapper.mapper_test (name text, age int, primary key (name))");
    }

    @Override
    public String getKeyspaceName() {
      return "mapper";
    }

    @Override
    public boolean isKeyspaceCreation() {
      return true;
    }

    @Override
    public boolean isKeyspaceDeletion() {
      return true;
    }
  }
}
