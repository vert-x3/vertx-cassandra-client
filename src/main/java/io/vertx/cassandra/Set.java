package io.vertx.cassandra;

import io.vertx.codegen.annotations.VertxGen;

import java.util.Iterator;

@VertxGen
public interface Set<T> extends java.util.Set<T> {

  @Override
  int size();

  @Override
  CassandraIterator<T> iterator();

  @Override
  boolean contains(Object o);
}
