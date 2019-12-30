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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;

import java.util.Objects;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
class Util {

  /**
   * Adapt {@link ListenableFuture} to Vert.x {@link Future}.
   *
   * The returned {@link Future} callbacks will be invoked on the provided {@code context}.
   */
  static <T> Future<T> toVertxFuture(ListenableFuture<T> listenableFuture, ContextInternal context) {
    Objects.requireNonNull(listenableFuture, "listenableFuture must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Promise<T> promise = context.promise();
    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        promise.complete(result);
      }

      @Override
      public void onFailure(Throwable t) {
        promise.fail(t);
      }
    });
    return promise.future();
  }

  /**
   * Set the {@code handler} on the given {@code future}, if the {@code handler} is not null.
   */
  static <T> void setHandler(Future<T> future, Handler<AsyncResult<T>> handler) {
    Objects.requireNonNull(future, "future must not be null");
    if (handler != null) {
      future.setHandler(handler);
    }
  }
}
