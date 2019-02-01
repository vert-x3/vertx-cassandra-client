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

import java.util.Objects;
import java.util.function.Function;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
class Util {

  /**
   * Invokes the {@code handler} on a given {@code context} when the {@code listenableFuture} succeeds or fails.
   */
  static <T> void handleOnContext(ListenableFuture<T> listenableFuture, Context context, Handler<AsyncResult<T>> handler) {
    handleOnContext(listenableFuture, context, Function.identity(), handler);
  }

  /**
   * Invokes the {@code handler} on a given {@code context} when the {@code listenableFuture} succeeds or fails.
   */
  static <I, O> void handleOnContext(ListenableFuture<I> listenableFuture, Context context, Function<I, O> converter, Handler<AsyncResult<O>> handler) {
    Objects.requireNonNull(listenableFuture, "listenableFuture must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(converter, "converter must not be null");
    if (handler != null) {
      Futures.addCallback(listenableFuture, new FutureCallback<I>() {
        @Override
        public void onSuccess(I result) {
          context.runOnContext(v -> handler.handle(Future.succeededFuture(converter.apply(result))));
        }

        @Override
        public void onFailure(Throwable t) {
          context.runOnContext(v -> handler.handle(Future.failedFuture(t)));
        }
      });
    }
  }
}
