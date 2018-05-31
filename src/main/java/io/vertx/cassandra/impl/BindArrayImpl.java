package io.vertx.cassandra.impl;

import io.vertx.cassandra.BindArray;

import java.util.ArrayList;

public class BindArrayImpl implements BindArray {

  public ArrayList<Object> list = new ArrayList<>(0);

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public BindArray add(Object o) {
    list.add(o);
    return this;
  }
}
