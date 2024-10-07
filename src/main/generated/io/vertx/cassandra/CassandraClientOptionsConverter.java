package io.vertx.cassandra;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.cassandra.CassandraClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.cassandra.CassandraClientOptions} original class using Vert.x codegen.
 */
public class CassandraClientOptionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, CassandraClientOptions obj) {
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
        case "username":
          if (member.getValue() instanceof String) {
            obj.setUsername((String)member.getValue());
          }
          break;
        case "password":
          if (member.getValue() instanceof String) {
            obj.setPassword((String)member.getValue());
          }
          break;
        case "tracingPolicy":
          if (member.getValue() instanceof String) {
            obj.setTracingPolicy(io.vertx.core.tracing.TracingPolicy.valueOf((String)member.getValue()));
          }
          break;
      }
    }
  }

   static void toJson(CassandraClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(CassandraClientOptions obj, java.util.Map<String, Object> json) {
    if (obj.getKeyspace() != null) {
      json.put("keyspace", obj.getKeyspace());
    }
    if (obj.getUsername() != null) {
      json.put("username", obj.getUsername());
    }
    if (obj.getPassword() != null) {
      json.put("password", obj.getPassword());
    }
    if (obj.getTracingPolicy() != null) {
      json.put("tracingPolicy", obj.getTracingPolicy().name());
    }
  }
}
