package io.vertx.cassandra;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.cassandra.CassandraClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.cassandra.CassandraClientOptions} original class using Vert.x codegen.
 */
public class CassandraClientOptionsConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, CassandraClientOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "contactPoints":
          if (member.getValue() instanceof JsonObject) {
            ((Iterable<java.util.Map.Entry<String, Object>>)member.getValue()).forEach(entry -> {
              if (entry.getValue() instanceof Number)
                obj.addContactPoint(entry.getKey(), ((Number)entry.getValue()).intValue());
            });
          }
          break;
        case "keyspace":
          if (member.getValue() instanceof String) {
            obj.setKeyspace((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(CassandraClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(CassandraClientOptions obj, java.util.Map<String, Object> json) {
    if (obj.getKeyspace() != null) {
      json.put("keyspace", obj.getKeyspace());
    }
  }
}
