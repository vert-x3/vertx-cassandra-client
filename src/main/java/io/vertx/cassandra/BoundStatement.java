package io.vertx.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;

/**
 * Analogy of {@link com.datastax.driver.core.BoundStatement}.
 */
@VertxGen
public interface BoundStatement extends Statement {

  /**
   * @param pos the position to set
   * @param val the value to set
   */
  @Fluent
  BoundStatement set(int pos, Object val);

  /**
   * @param name the param name to set
   * @param val the value to set
   */
  @Fluent
  BoundStatement set(String name, Object val);

  /**
   * @see Statement#setConsistencyLevel(ConsistencyLevel)
   */
  @Override
  BoundStatement setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * @see Statement#getConsistencyLevel()
   */
  @Override
  ConsistencyLevel getConsistencyLevel();
}
