package io.vertx.cassandra.impl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class Util {

  /**
   * Transform the guava future to a Vert.x future.
   *
   * @param future        the guava future
   * @param vertx the Vert.x instance
   * @param <T>           the future type
   * @return the Vert.x future
   */
  static <T> Future<T> toVertxFuture(ListenableFuture<T> future, Vertx vertx) {
    Context context = vertx.getOrCreateContext();
    Future<T> vertxFuture = Future.future();
    Futures.addCallback(future, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        vertxFuture.complete(result);
      }

      @Override
      public void onFailure(Throwable t) {
        vertxFuture.fail(t);
      }
    }, command -> context.runOnContext(v -> command.run()));
    return vertxFuture;
  }
}
