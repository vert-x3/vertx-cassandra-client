/*
 * Copyright (c) 2014 Red Hat, Inc. and others
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

package io.vertx.cassandra;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Converter for {@link io.vertx.cassandra.CassandraClientOptions}.
 *
 * NOTE: This class has been automatically generated from the {@link io.vertx.cassandra.CassandraClientOptions} original class using Vert.x codegen.
 */
public class CassandraClientOptionsConverter {

  public static void fromJson(JsonObject json, CassandraClientOptions obj) {
    if (json.getValue("contactPoints") instanceof JsonArray) {
      java.util.ArrayList<java.lang.String> list = new java.util.ArrayList<>();
      json.getJsonArray("contactPoints").forEach( item -> {
        if (item instanceof String)
          list.add((String)item);
      });
      obj.setContactPoints(list);
    }
    if (json.getValue("port") instanceof Number) {
      obj.setPort(((Number)json.getValue("port")).intValue());
    }
  }

  public static void toJson(CassandraClientOptions obj, JsonObject json) {
  }
}