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
package io.vertx.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * It is like {@link com.datastax.driver.core.Row}, but adopted for Vert.x.
 */
@VertxGen
public interface Row {

  /* Vert.x codegen compatible methods */

  boolean isNull(int i);

  boolean getBool(int i);

  byte getByte(int i);

  short getShort(int i);

  int getInt(int i);

  long getLong(int i);

  long getTime(int i);

  float getFloat(int i);

  double getDouble(int i);

  Buffer getBytesUnsafe(int i);

  Buffer getBytes(int i);

  String getString(int i);

  boolean isNull(String name);

  boolean getBool(String name);

  byte getByte(String name);

  short getShort(String name);

  int getInt(String name);

  long getLong(String name);

  long getTime(String name);

  float getFloat(String name);

  double getDouble(String name);

  Buffer getBytesUnsafe(String name);

  Buffer getBytes(String name);

  String getString(String name);

  /* Non compatible with Vert.x codegen method */

  // #TODO consider which of them should be removed or somehow adopetd

  @GenIgnore
  Date getTimestamp(int i);

  @GenIgnore
  LocalDate getDate(int i);

  @GenIgnore
  Date getTimestamp(String name);

  @GenIgnore
  LocalDate getDate(String name);

  @GenIgnore
  ColumnDefinitions getColumnDefinitions();

  @GenIgnore
  Token getToken(int i);

  @GenIgnore
  Token getToken(String name);

  @GenIgnore
  Token getPartitionKeyToken();

  @GenIgnore
  BigInteger getVarint(int i);

  @GenIgnore
  BigDecimal getDecimal(int i);

  @GenIgnore
  UUID getUUID(int i);

  @GenIgnore
  InetAddress getInet(int i);

  @GenIgnore
  <T> List<T> getList(int i, Class<T> elementsClass);

  @GenIgnore
  <T> List<T> getList(int i, TypeToken<T> elementsType);

  @GenIgnore
  <T> Set<T> getSet(int i, Class<T> elementsClass);

  @GenIgnore
  <T> Set<T> getSet(int i, TypeToken<T> elementsType);

  @GenIgnore
  <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass);

  @GenIgnore
  <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType);

  @GenIgnore
  UDTValue getUDTValue(int i);

  @GenIgnore
  TupleValue getTupleValue(int i);

  @GenIgnore
  Object getObject(int i);

  @GenIgnore
  <T> T get(int i, Class<T> targetClass);

  @GenIgnore
  <T> T get(int i, TypeToken<T> targetType);

  @GenIgnore
  <T> T get(int i, TypeCodec<T> codec);

  @GenIgnore
  BigInteger getVarint(String name);

  @GenIgnore
  BigDecimal getDecimal(String name);

  @GenIgnore
  UUID getUUID(String name);

  @GenIgnore
  InetAddress getInet(String name);

  @GenIgnore
  <T> List<T> getList(String name, Class<T> elementsClass);

  @GenIgnore
  <T> List<T> getList(String name, TypeToken<T> elementsType);

  @GenIgnore
  <T> Set<T> getSet(String name, Class<T> elementsClass);

  @GenIgnore
  <T> Set<T> getSet(String name, TypeToken<T> elementsType);

  @GenIgnore
  <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);

  @GenIgnore
  <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType);

  @GenIgnore
  UDTValue getUDTValue(String name);

  @GenIgnore
  TupleValue getTupleValue(String name);

  @GenIgnore
  Object getObject(String name);

  @GenIgnore
  <T> T get(String name, Class<T> targetClass);

  @GenIgnore
  <T> T get(String name, TypeToken<T> targetType);

  @GenIgnore
  <T> T get(String name, TypeCodec<T> codec);
}
