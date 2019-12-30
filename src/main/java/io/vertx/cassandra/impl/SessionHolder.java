/*
 * Copyright 2019 Red Hat, Inc.
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

import com.datastax.oss.driver.api.core.CqlSession;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.shareddata.Shareable;

import java.util.Objects;

/**
 * @author Thomas Segismont
 */
class SessionHolder implements Shareable {

  final TaskQueue connectionQueue;
  final CqlSession session;
  final int refCount;

  SessionHolder() {
    connectionQueue = new TaskQueue();
    session = null;
    refCount = 1;
  }

  private SessionHolder(TaskQueue connectionQueue, CqlSession session, int refCount) {
    this.connectionQueue = connectionQueue;
    this.session = session;
    this.refCount = refCount;
  }

  SessionHolder connected(CqlSession session) {
    Objects.requireNonNull(session);
    if (this.session != null) {
      throw new IllegalStateException();
    }
    return new SessionHolder(connectionQueue, session, refCount);
  }

  SessionHolder increment() {
    return new SessionHolder(connectionQueue, session, refCount + 1);
  }

  SessionHolder decrement() {
    if (refCount < 1) {
      throw new IllegalArgumentException();
    }
    return new SessionHolder(connectionQueue, session, refCount - 1);
  }
}
