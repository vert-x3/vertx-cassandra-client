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

import io.vertx.cassandra.Row;

public class RowImpl implements Row {

  com.datastax.driver.core.Row datastaxRow;

  public RowImpl() {
  }

  public RowImpl(com.datastax.driver.core.Row datastaxRow) {
    this.datastaxRow = datastaxRow;
  }

  public String getString(int i) {
    return datastaxRow.getString(i);
  }

  public String getString(String columnName) {
    return datastaxRow.getString(columnName);
  }
}
