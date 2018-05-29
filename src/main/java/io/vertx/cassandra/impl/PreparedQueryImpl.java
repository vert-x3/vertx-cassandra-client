package io.vertx.cassandra.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import io.vertx.cassandra.ExecutableQuery;
import io.vertx.cassandra.PreparedQuery;
import io.vertx.core.json.JsonArray;

public class PreparedQueryImpl implements PreparedQuery {

  private final PreparedStatement datastaxStatement;

  public PreparedQueryImpl(PreparedStatement datastaxStatement) {
    this.datastaxStatement = datastaxStatement;
  }

  @Override
  public ExecutableQuery bind(JsonArray params) {
    BoundStatement filled = datastaxStatement.bind(params.getList().toArray());
    return new ExecutableQueryImpl(filled);
  }
}
