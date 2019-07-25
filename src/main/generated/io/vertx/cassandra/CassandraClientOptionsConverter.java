package io.vertx.cassandra;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.cassandra.CassandraClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.cassandra.CassandraClientOptions} original class using Vert.x codegen.
 */
public class CassandraClientOptionsConverter implements JsonCodec<CassandraClientOptions, JsonObject> {

  public static final CassandraClientOptionsConverter INSTANCE = new CassandraClientOptionsConverter();

  @Override public JsonObject encode(CassandraClientOptions value) { return (value != null) ? value.toJson() : null; }

  @Override public CassandraClientOptions decode(JsonObject value) { return (value != null) ? new CassandraClientOptions(value) : null; }

  @Override public Class<CassandraClientOptions> getTargetClass() { return CassandraClientOptions.class; }

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
        case "keyspace":
          if (member.getValue() instanceof String) {
            obj.setKeyspace((String)member.getValue());
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
    if (obj.getContactPoints() != null) {
      JsonArray array = new JsonArray();
      obj.getContactPoints().forEach(item -> array.add(item));
      json.put("contactPoints", array);
    }
    if (obj.getKeyspace() != null) {
      json.put("keyspace", obj.getKeyspace());
    }
  }
}
