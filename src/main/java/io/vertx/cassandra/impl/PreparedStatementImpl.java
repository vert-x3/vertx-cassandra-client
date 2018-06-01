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

import io.vertx.cassandra.BindArray;
import io.vertx.cassandra.BoundStatement;
import io.vertx.cassandra.PreparedStatement;

public class PreparedStatementImpl implements PreparedStatement {

  private final com.datastax.driver.core.PreparedStatement datastaxStatement;

  public PreparedStatementImpl(com.datastax.driver.core.PreparedStatement datastaxStatement) {
    this.datastaxStatement = datastaxStatement;
  }

  @Override
  public BoundStatement bind(BindArray params) {
    com.datastax.driver.core.BoundStatement filled = datastaxStatement.bind(((BindArrayImpl) params).list.toArray());
    return new BoundStatementImpl(filled);
  }

  @Override
  public BoundStatement bind() {
    return new BoundStatementImpl(datastaxStatement.bind());
  }
}
