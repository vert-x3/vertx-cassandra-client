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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A base class, which should be used for in all test where Cassandra service is required.
 */
public class CassandraServiceBase {

  Vertx vertx = Vertx.vertx();
  static final String HOST = "localhost";
  static final int NATIVE_TRANSPORT_PORT = 9142;
  private static final int BATCH_INSERT_SIZE = 10_000;

  @Before
  public void before() throws IOException, TTransportException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra_config.yaml");

    Cluster.Builder builder = Cluster.builder()
      .addContactPoint(HOST)
      .withPort(NATIVE_TRANSPORT_PORT);

    Session connect = builder.build().connect();

    connect.execute("CREATE KEYSPACE IF NOT EXISTS playlist WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
    connect.execute("create table playlist.track_by_id (track text, artist text, track_id UUID, track_length_in_seconds int, genre text, music_file text, primary key (track_id));");
    connect.execute("create table playlist.track_by_artist (track text, artist text, track_id UUID, track_length_in_seconds int, genre text,music_file text, starred boolean, primary key (artist, track, track_id)) WITH caching = {'rows_per_partition':'100'};");
    connect.execute("create table playlist.track_by_genre (track text, artist text, track_id UUID, track_length_in_seconds int, genre text,music_file text, starred boolean, primary key (genre, artist, track, track_id)) WITH caching = {'rows_per_partition':'100'};");
    connect.execute("create table playlist.artists_by_first_letter (first_letter text, artist text, primary key (first_letter, artist));");

    PreparedStatement byId = connect.prepare("INSERT INTO playlist.track_by_id (track_id, genre, artist, track, track_length_in_seconds, music_file) VALUES (?, ?, ?, ?, ?, ?)");
    PreparedStatement byArtist = connect.prepare("INSERT INTO playlist.track_by_artist (track_id, genre, artist, track, track_length_in_seconds, music_file) VALUES (?, ?, ?, ?, ?, ?)");
    PreparedStatement byGenre = connect.prepare("INSERT INTO playlist.track_by_genre (track_id, genre, artist, track, track_length_in_seconds, music_file) VALUES (?, ?, ?, ?, ?, ?)");
    PreparedStatement byLetter = connect.prepare("INSERT INTO playlist.artists_by_first_letter (first_letter, artist) VALUES (?, ?)");

    List<String[]> songs = csvLines("songs.csv");
    List<String[]> artists = csvLines("artists.csv");

    Lists.partition(
      songs, BATCH_INSERT_SIZE
    ).forEach(batch -> {
      insertSongBatch(connect, byId, batch);
      insertSongBatch(connect, byArtist, batch);
      insertSongBatch(connect, byGenre, batch);
    });

    Lists.partition(
      artists, BATCH_INSERT_SIZE
    ).forEach(batch -> {
      BatchStatement batchStatement = new BatchStatement();
      batch.forEach(each -> batchStatement.add(byLetter.bind(each[0], each[1])));
      connect.execute(batchStatement);
    });

  }

  private List<String[]> csvLines(String path) {
    return Arrays.stream(
      vertx.fileSystem().readFileBlocking(path).toString().split("\n")
    ).map(line -> line.split("\\|")).collect(Collectors.toList());
  }

  private void insertSongBatch(Session connect, PreparedStatement byId, List<String[]> batch) {
    BatchStatement batchStatement = new BatchStatement();
    batch.forEach(each -> {
      batchStatement.add(byId.bind(UUID.fromString(each[0]), each[1], each[2], each[3], Integer.parseInt(each[4]), each[5]));
    });
    connect.execute(batchStatement);
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}
