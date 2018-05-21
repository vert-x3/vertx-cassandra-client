package io.vertx.cassandra;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

/**
 * A {@link ReadStream} for {@link Row} consumption.
 */
@VertxGen
public interface CassandraRowStream extends ReadStream<Row> {

  @Override
  CassandraRowStream exceptionHandler(Handler<Throwable> handler);

  @Override
  CassandraRowStream handler(Handler<Row> handler);

  @Override
  CassandraRowStream pause();

  @Override
  CassandraRowStream resume();

  @Override
  CassandraRowStream endHandler(Handler<Void> handler);

}
