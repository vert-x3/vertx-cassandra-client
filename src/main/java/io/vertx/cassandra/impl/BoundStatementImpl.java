package io.vertx.cassandra.impl;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import io.vertx.cassandra.BoundStatement;

public class BoundStatementImpl extends StatementImpl implements BoundStatement {

  public BoundStatementImpl(com.datastax.driver.core.BoundStatement boundStatement) {
    this.statement = boundStatement;
  }

  @Override
  public io.vertx.cassandra.BoundStatement set(int pos, Object val) {
    ((com.datastax.driver.core.BoundStatement)statement).set(pos, val, CodecRegistry.DEFAULT_INSTANCE.codecFor(val));
    return this;
  }

  @Override
  public io.vertx.cassandra.BoundStatement set(String name, Object val) {
    ((com.datastax.driver.core.BoundStatement)statement).set(name, val, CodecRegistry.DEFAULT_INSTANCE.codecFor(val));
    return this;
  }

  @Override
  public io.vertx.cassandra.BoundStatement setConsistencyLevel(ConsistencyLevel consistency) {
    super.setConsistencyLevel(consistency);
    return this;
  }

  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return super.getConsistencyLevel();
  }
}
