package io.vertx.cassandra.impl;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import io.vertx.cassandra.ExecutableQuery;

public class ExecutableQueryImpl implements ExecutableQuery {

  Statement dataStaxBoundStatement;

  public ExecutableQueryImpl(Statement filled) {
    dataStaxBoundStatement = filled;
  }

  @Override
  public ExecutableQuery setConsistencyLevel(ConsistencyLevel consistency) {
    dataStaxBoundStatement.setConsistencyLevel(consistency);
    return this;
  }

  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return dataStaxBoundStatement.getConsistencyLevel();
  }
}
