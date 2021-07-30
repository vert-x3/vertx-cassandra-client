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

package io.vertx.cassandra.impl.tracing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;

import static java.util.stream.Collectors.joining;

public class QueryRequest {

  public final String address;
  public final String keyspace;
  public final String cql;

  public QueryRequest(CqlSession session, Statement statement) {
    Metadata metadata = session.getMetadata();
    address = metadata.getNodes().values().stream()
      .map(Node::getEndPoint)
      .map(EndPoint::asMetricPrefix)
      .collect(joining(",", "[", "]"));
    keyspace = session.getKeyspace().map(CqlIdentifier::toString).orElse("");
    if (statement instanceof SimpleStatement) {
      SimpleStatement simpleStatement = (SimpleStatement) statement;
      cql = simpleStatement.getQuery();
    } else if (statement instanceof BoundStatement) {
      BoundStatement boundStatement = (BoundStatement) statement;
      cql = boundStatement.getPreparedStatement().getQuery();
    } else if (statement instanceof BatchStatement) {
      cql = "_batch_";
    } else {
      cql = "unknown statement type";
    }
  }

  @Override
  public String toString() {
    return "QueryRequest{" +
      "address='" + address + '\'' +
      ", keyspace='" + keyspace + '\'' +
      ", cql='" + cql + '\'' +
      '}';
  }
}
