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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;
import io.vertx.cassandra.Row;
import io.vertx.core.buffer.Buffer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RowImpl implements Row {

  com.datastax.driver.core.Row datastaxRow;

  public RowImpl() {
  }

  public RowImpl(com.datastax.driver.core.Row datastaxRow) {
    this.datastaxRow = datastaxRow;
  }

  @Override
  public boolean isNull(int i) {
    return datastaxRow.isNull(i);
  }

  @Override
  public boolean getBool(int i) {
    return datastaxRow.getBool(i);
  }

  @Override
  public byte getByte(int i) {
    return datastaxRow.getByte(i);
  }

  @Override
  public short getShort(int i) {
    return datastaxRow.getShort(i);
  }

  @Override
  public int getInt(int i) {
    return datastaxRow.getInt(i);
  }

  @Override
  public long getLong(int i) {
    return datastaxRow.getLong(i);
  }

  @Override
  public long getTime(int i) {
    return datastaxRow.getTime(i);
  }

  @Override
  public float getFloat(int i) {
    return datastaxRow.getFloat(i);
  }

  @Override
  public double getDouble(int i) {
    return datastaxRow.getDouble(i);
  }

  @Override
  public Buffer getBytesUnsafe(int i) {
    return null;
  }

  @Override
  public Buffer getBytes(int i) {
    return Buffer.buffer(datastaxRow.getBytes(i).array());
  }

  public String getString(int i) {
    return datastaxRow.getString(i);
  }

  @Override
  public boolean isNull(String name) {
    return datastaxRow.isNull(name);
  }

  @Override
  public boolean getBool(String name) {
    return datastaxRow.getBool(name);
  }

  @Override
  public byte getByte(String name) {
    return datastaxRow.getByte(name);
  }

  @Override
  public short getShort(String name) {
    return datastaxRow.getShort(name);
  }

  @Override
  public int getInt(String name) {
    return datastaxRow.getInt(name);
  }

  @Override
  public long getLong(String name) {
    return datastaxRow.getLong(name);
  }

  @Override
  public long getTime(String name) {
    return datastaxRow.getTime(name);
  }

  @Override
  public float getFloat(String name) {
    return datastaxRow.getFloat(name);
  }

  @Override
  public double getDouble(String name) {
    return datastaxRow.getDouble(name);
  }

  @Override
  public Buffer getBytesUnsafe(String name) {
    return Buffer.buffer(datastaxRow.getBytesUnsafe(name).array());
  }

  @Override
  public Buffer getBytes(String name) {
    return Buffer.buffer(datastaxRow.getBytes(name).array());
  }

  public String getString(String columnName) {
    return datastaxRow.getString(columnName);
  }

  @Override
  public Date getTimestamp(int i) {
    return datastaxRow.getTimestamp(i);
  }

  @Override
  public LocalDate getDate(int i) {
    return datastaxRow.getDate(i);
  }

  @Override
  public Date getTimestamp(String name) {
    return datastaxRow.getTimestamp(name);
  }

  @Override
  public LocalDate getDate(String name) {
    return datastaxRow.getDate(name);
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return datastaxRow.getColumnDefinitions();
  }

  @Override
  public Token getToken(int i) {
    return datastaxRow.getToken(i);
  }

  @Override
  public Token getToken(String name) {
    return datastaxRow.getToken(name);
  }

  @Override
  public Token getPartitionKeyToken() {
    return datastaxRow.getPartitionKeyToken();
  }

  @Override
  public BigInteger getVarint(int i) {
    return null;
  }

  @Override
  public BigDecimal getDecimal(int i) {
    return datastaxRow.getDecimal(i);
  }

  @Override
  public UUID getUUID(int i) {
    return datastaxRow.getUUID(i);
  }

  @Override
  public InetAddress getInet(int i) {
    return datastaxRow.getInet(i);
  }

  @Override
  public <T> List<T> getList(int i, Class<T> elementsClass) {
    return datastaxRow.getList(i, elementsClass);
  }

  @Override
  public <T> List<T> getList(int i, TypeToken<T> elementsType) {
    return datastaxRow.getList(i, elementsType);
  }

  @Override
  public <T> Set<T> getSet(int i, Class<T> elementsClass) {
    return datastaxRow.getSet(i, elementsClass);
  }

  @Override
  public <T> java.util.Set<T> getSet(int i, TypeToken<T> elementsType) {
    return datastaxRow.getSet(i, elementsType);
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
    return datastaxRow.getMap(i, keysClass, valuesClass);
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
    return datastaxRow.getMap(i, keysType, valuesType);
  }

  @Override
  public UDTValue getUDTValue(int i) {
    return datastaxRow.getUDTValue(i);
  }

  @Override
  public TupleValue getTupleValue(int i) {
    return datastaxRow.getTupleValue(i);
  }

  @Override
  public Object getObject(int i) {
    return datastaxRow.getObject(i);
  }

  @Override
  public <T> T get(int i, Class<T> targetClass) {
    return datastaxRow.get(i, targetClass);
  }

  @Override
  public <T> T get(int i, TypeToken<T> targetType) {
    return datastaxRow.get(i, targetType);
  }

  @Override
  public <T> T get(int i, TypeCodec<T> codec) {
    return datastaxRow.get(i, codec);
  }

  @Override
  public BigInteger getVarint(String name) {
    return datastaxRow.getVarint(name);
  }

  @Override
  public BigDecimal getDecimal(String name) {
    return datastaxRow.getDecimal(name);
  }

  @Override
  public UUID getUUID(String name) {
    return datastaxRow.getUUID(name);
  }

  @Override
  public InetAddress getInet(String name) {
    return datastaxRow.getInet(name);
  }

  @Override
  public <T> List<T> getList(String name, Class<T> elementsClass) {
    return datastaxRow.getList(name, elementsClass);
  }

  @Override
  public <T> List<T> getList(String name, TypeToken<T> elementsType) {
    return datastaxRow.getList(name, elementsType);
  }

  @Override
  public <T> java.util.Set<T> getSet(String name, Class<T> elementsClass) {
    return datastaxRow.getSet(name, elementsClass);
  }

  @Override
  public <T> java.util.Set<T> getSet(String name, TypeToken<T> elementsType) {
    return datastaxRow.getSet(name, elementsType);
  }

  @Override
  public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
    return datastaxRow.getMap(name, keysClass, valuesClass);
  }

  @Override
  public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
    return datastaxRow.getMap(name, keysType, valuesType);
  }

  @Override
  public UDTValue getUDTValue(String name) {
    return datastaxRow.getUDTValue(name);
  }

  @Override
  public TupleValue getTupleValue(String name) {
    return datastaxRow.getTupleValue(name);
  }

  @Override
  public Object getObject(String name) {
    return datastaxRow.getObject(name);
  }

  @Override
  public <T> T get(String name, Class<T> targetClass) {
    return datastaxRow.get(name, targetClass);
  }

  @Override
  public <T> T get(String name, TypeToken<T> targetType) {
    return datastaxRow.get(name, targetType);
  }

  @Override
  public <T> T get(String name, TypeCodec<T> codec) {
    return datastaxRow.get(name, codec);
  }

}
