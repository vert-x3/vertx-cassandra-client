package io.vertx.cassandra;

import io.vertx.cassandra.impl.BindArrayImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;

/**
 * Array, which can store any {@link Object}, should be used for calling {@link PreparedQuery#bind(BindArray)}
 */
@VertxGen
public interface BindArray  {

  /**
   * @return a new empty array
   */
  static BindArray create(){
    return new BindArrayImpl();
  }

  /**
   * @return the array size
   */
  int size();

  /**
   * @param o the object to add
   * @return current instance
   */
  @Fluent
  BindArray add(Object o);
}
