/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.cassandra.impl;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * @author Thomas Segismont
 */
@RunWith(VertxUnitRunner.class)
public class UtilTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Test
  public void testSuccess(TestContext testContext) {
    Vertx vertx = rule.vertx();
    SettableFuture<String> future = SettableFuture.create();
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    Util.handleOnContext(future, context).setHandler(testContext.asyncAssertSuccess(value -> {
      testContext.assertTrue(context == Vertx.currentContext());
      testContext.assertEquals("foo", value);
    }));
    future.set("foo");
  }

  @Test
  public void testFailure(TestContext testContext) {
    Vertx vertx = rule.vertx();
    SettableFuture<String> future = SettableFuture.create();
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    Exception expected = new Exception();
    Util.handleOnContext(future, context).setHandler(testContext.asyncAssertFailure(throwable -> {
      testContext.assertTrue(context == Vertx.currentContext());
      testContext.assertTrue(expected == throwable);
    }));
    future.setException(expected);
  }
  
  @Test
  public void testConverterInvokedOnContext(TestContext testContext) {
    Vertx vertx = rule.vertx();
    SettableFuture<String> future = SettableFuture.create();
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    Function<String, Integer> converter = s -> {
      testContext.assertTrue(context == Vertx.currentContext());
      return s.length();
    };
    Util.handleOnContext(future, context, converter).setHandler(testContext.asyncAssertSuccess(value -> {
      testContext.assertTrue(context == Vertx.currentContext());
      testContext.assertEquals(3, value);
    }));
    future.set("foo");
  }
}
