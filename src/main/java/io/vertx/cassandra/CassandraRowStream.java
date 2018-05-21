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
  ReadStream<Row> exceptionHandler(Handler<Throwable> handler);

  @Override
  ReadStream<Row> handler(Handler<Row> handler);

  @Override
  ReadStream<Row> pause();

  @Override
  ReadStream<Row> resume();

  @Override
  ReadStream<Row> endHandler(Handler<Void> handler);

}
