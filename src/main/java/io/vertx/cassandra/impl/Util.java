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

import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * @author Pavel Drankou
 * @author Thomas Segismont
 */
class Util {

  /**
   * Invokes the {@code handler} on a given {@code context} when the {@code listenableFuture} succeeds or fails.
   */
  static <T> void handleOnContext(CompletionStage<T> completionStage, Context context, Handler<AsyncResult<T>> handler) {
    handleOnContext(completionStage, context, Function.identity(), handler);
  }

  /**
   * Invokes the {@code handler} on a given {@code context} when the {@code listenableFuture} succeeds or fails.
   */
  static <I, O> void handleOnContext(CompletionStage<I> completionStage, Context context, Function<I, O> converter, Handler<AsyncResult<O>> handler) {
    Objects.requireNonNull(completionStage, "completionStage must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(converter, "converter must not be null");
    Objects.requireNonNull(handler, "handler must not be null");
    completionStage
      .whenComplete((result, err) -> {
        if (err != null) {
          context.runOnContext(v -> handler.handle(Future.failedFuture(err)));
        } else {
          context.runOnContext(v -> handler.handle(Future.succeededFuture(converter.apply(result))));
        }
      });
    }

  /**
   * Adapt {@link CompletionStage} to Vert.x {@link Future}.
   * <p>
   * The returned {@link Future} callbacks will be invoked on the provided {@code context}.
   */
  static <T> Future<T> toVertxFuture(CompletionStage<T> completionStage, ContextInternal context) {
    Objects.requireNonNull(completionStage, "completionStage must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Promise<T> promise = context.promise();
    completionStage
      .whenComplete((result, err) -> {
        if (err != null) {
          promise.fail(err);
        } else {
          promise.complete(result);
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
