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
package io.vertx.cassandra;

import io.vertx.cassandra.impl.BindArrayImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;

/**
 * Array, which can store any {@link Object}, should be used for calling {@link PreparedStatement#bind(BindArray)}
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
