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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
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
