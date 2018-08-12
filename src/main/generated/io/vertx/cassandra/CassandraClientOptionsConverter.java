package io.vertx.cassandra;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.cassandra.CassandraClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.cassandra.CassandraClientOptions} original class using Vert.x codegen.
 */
public class CassandraClientOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, CassandraClientOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "contactPoints":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<java.lang.String> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setContactPoints(list);
          }
          break;
        case "port":
          if (member.getValue() instanceof Number) {
            obj.setPort(((Number)member.getValue()).intValue());
          }
          break;
      }
    }
  }

  public static void toJson(CassandraClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(CassandraClientOptions obj, java.util.Map<String, Object> json) {
  }
}
