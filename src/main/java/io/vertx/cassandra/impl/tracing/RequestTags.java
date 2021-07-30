/*
 * Copyright 2021 Red Hat, Inc.
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

package io.vertx.cassandra.impl.tracing;

import io.vertx.core.spi.tracing.TagExtractor;

import java.util.function.Function;

public enum RequestTags {

  // Generic
  PEER_ADDRESS("peer.address", q -> q.address),
  SPAN_KIND("span.kind", q -> "client"),

  // DB
  // See https://github.com/opentracing/specification/blob/master/semantic_conventions.md

  DB_INSTANCE("db.instance", q -> q.keyspace),
  DB_STATEMENT("db.statement", q -> q.cql),
  DB_TYPE("db.type", q -> "cassandra");

  final String name;
  final Function<QueryRequest, String> fn;

  RequestTags(String name, Function<QueryRequest, String> fn) {
    this.name = name;
    this.fn = fn;
  }

  public static final TagExtractor<QueryRequest> REQUEST_TAG_EXTRACTOR = new TagExtractor<QueryRequest>() {

    private final RequestTags[] TAGS = RequestTags.values();

    @Override
    public int len(QueryRequest obj) {
      return TAGS.length;
    }

    @Override
    public String name(QueryRequest obj, int index) {
      return TAGS[index].name;
    }

    @Override
    public String value(QueryRequest obj, int index) {
      return TAGS[index].fn.apply(obj);
    }
  };
}
