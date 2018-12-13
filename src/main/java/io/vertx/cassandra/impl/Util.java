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

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
class Util {

  /**
   * Invokes the {@code handler} on a given {@code context} when the {@code listenableFuture} succeeds or fails.
   */
  static <T> void handleOnContext(ListenableFuture<T> listenableFuture, Context context, Handler<AsyncResult<T>> handler) {
    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        context.runOnContext(v -> handler.handle(Future.succeededFuture(result)));
      }

      @Override
      public void onFailure(Throwable t) {
        context.runOnContext(v -> handler.handle(Future.failedFuture(t)));
      }
    });
  }
}
