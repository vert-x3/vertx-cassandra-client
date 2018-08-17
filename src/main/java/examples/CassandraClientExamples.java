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
package examples;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.cassandra.CassandraRowStream;
import io.vertx.cassandra.ResultSet;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;

import java.util.List;

public class CassandraClientExamples {

  public void specifyingNodes(Vertx vertx) {
    CassandraClientOptions options = new CassandraClientOptions();
    options
      .addContactPoint("node1.address")
      .addContactPoint("node2.address")
      .addContactPoint("node3.address");
    CassandraClient client = CassandraClient.createShared(vertx, options);
  }

  public void connecting(CassandraClient cassandraClient) {
    cassandraClient.connect(connect -> {
      if (connect.succeeded()) {
        System.out.println("Just connected");
      } else {
        System.out.println("Unable to connect");
        connect.cause().printStackTrace();
      }
    });
  }

  public void disconnecting(CassandraClient cassandraClient) {
    cassandraClient.disconnect(disconnect -> {
      if (disconnect.succeeded()) {
        System.out.println("Just disconnected");
      } else {
        System.out.println("Unable to disconnect");
        disconnect.cause().printStackTrace();
      }
    });
  }

  public void lowLevelQuerying(CassandraClient cassandraClient) {
    cassandraClient.execute("SELECT * FROM my_keyspace.my_table where my_key = 'my_value'", execute -> {
      if (execute.succeeded()) {
        ResultSet resultSet = execute.result();

        resultSet.one(one -> {
          if (one.succeeded()) {
            Row row = one.result();
            System.out.println("One row successfully fetched");
          } else {
            System.out.println("Unable to fetch a row");
            one.cause().printStackTrace();
          }
        });

        resultSet.fetchMoreResults(fetchMoreResults -> {
          if (fetchMoreResults.succeeded()) {
            int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
            System.out.println("Now we have " + availableWithoutFetching + " rows fetched, but not consumed!");
            if (resultSet.isFullyFetched()) {
              System.out.println("The result is fully fetched, we don't need to call this method for one more time!");
            } else {
              System.out.println("The result still does not fully fetched");
            }
          } else {
            System.out.println("Unable to fetch more results");
            fetchMoreResults.cause().printStackTrace();
          }
        });

      } else {
        System.out.println("Unable to execute the query");
        execute.cause().printStackTrace();
      }
    });
  }

  public void streamingViaHttp(Vertx vertx, CassandraClient cassandraClient, HttpServerResponse response) {
    cassandraClient.queryStream("SELECT my_string_col FROM my_keyspace.my_table where my_key = 'my_value'", queryStream -> {
      if (queryStream.succeeded()) {
        CassandraRowStream stream = queryStream.result();

        // resume stream when queue is ready to accept buffers again
        response.drainHandler(v -> stream.resume());

        stream.handler(row -> {
          String value = row.getString("my_string_col");
          response.write(value);

          // pause row stream when we buffer queue is full
          if (response.writeQueueFull()) {
            stream.pause();
          }
        });

        // end request when we reached end of the stream
        stream.endHandler(end -> response.end());

      } else {
        queryStream.cause().printStackTrace();
        // response with internal server error if we are not able to execute given query
        response
          .setStatusCode(500)
          .end("Unable to execute the query");
      }
    });
  }

  public void fetchAll(CassandraClient cassandraClient) {
    cassandraClient.executeWithFullFetch("SELECT * FROM my_keyspace.my_table where my_key = 'my_value'", executeWithFullFetch -> {
      if (executeWithFullFetch.succeeded()) {
        List<Row> rows = executeWithFullFetch.result();
        for (Row row : rows) {
          // handle each row here
        }
      } else {
        System.out.println("Unable to execute the query");
        executeWithFullFetch.cause().printStackTrace();
      }
    });
  }

  public void prepareQuery(CassandraClient cassandraClient) {
    cassandraClient.prepare("SELECT * FROM my_keyspace.my_table where my_key = ? ", preparedStatementResult -> {
      if (preparedStatementResult.succeeded()) {
        System.out.println("The query has successfully been prepared");
        PreparedStatement preparedStatement = preparedStatementResult.result();
        // now you can use this PreparedStatement object for the next queries
      } else {
        System.out.println("Unable to prepare the query");
        preparedStatementResult.cause().printStackTrace();
      }
    });
  }

  public void usingPreparedStatementFuture(CassandraClient cassandraClient, PreparedStatement preparedStatement) {
    // You can execute you prepared statement using any way to execute queries.

    // Low level fetch API
    cassandraClient.execute(preparedStatement.bind("my_value"), done -> {
      ResultSet results = done.result();
      // handle results here
    });

    // Bulk fetching API
    cassandraClient.executeWithFullFetch(preparedStatement.bind("my_value"), done -> {
      List<Row> results = done.result();
      // handle results here
    });

    // Streaming API
    cassandraClient.queryStream(preparedStatement.bind("my_value"), done -> {
      CassandraRowStream results = done.result();
      // handle results here
    });
  }

  public void batching(CassandraClient cassandraClient) {
    BatchStatement batchStatement = new BatchStatement();
    batchStatement.add(new SimpleStatement("INSERT INTO NAMES (name) VALUES ('Pavel')"));
    batchStatement.add(new SimpleStatement("INSERT INTO NAMES (name) VALUES ('Thomas')"));
    batchStatement.add(new SimpleStatement("INSERT INTO NAMES (name) VALUES ('Julien')"));

    cassandraClient.execute(batchStatement, result -> {
      if (result.succeeded()) {
        System.out.println("The given batch executed successfully");
      } else {
        System.out.println("Unable to execute the batch");
        result.cause().printStackTrace();
      }
    });
  }
}
