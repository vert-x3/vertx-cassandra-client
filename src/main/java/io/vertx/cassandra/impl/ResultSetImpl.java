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
package io.vertx.cassandra.impl;

import io.vertx.cassandra.CassandraIterator;
import io.vertx.cassandra.ResultSet;
import io.vertx.cassandra.Row;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ResultSetImpl implements ResultSet {

  com.datastax.driver.core.ResultSet resultSet;
  List<Row> rows;

  public ResultSetImpl() {
  }

  ResultSetImpl(com.datastax.driver.core.ResultSet resultSet) {
    this.resultSet = resultSet;
    rows = resultSet.all().stream().map(RowImpl::new).collect(Collectors.toList());
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public CassandraIterator<Row> iterator() {
    return new CassandraIterator<Row>() {
      Iterator<Row> iterator = rows.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Row next() {
        return iterator.next();
      }
    };
  }


}
