/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.util;

import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

/**
 * A thread-safe lazy initializer.
 * @param <T>
 */
public class Lazy<T> {
    private final FutureTask<T> task;

    public Lazy(Supplier<T> supplier) {
        this.task = new FutureTask<>(supplier::get);
    }

    public T get() {
        task.run(); // idempotent
        try {
            return task.get();
        } catch (Exception e) {
            throw new RuntimeException("Lazy initialization failed", e);
        }
    }
}
