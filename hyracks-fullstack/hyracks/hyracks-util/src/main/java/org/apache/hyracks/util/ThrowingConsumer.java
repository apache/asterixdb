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
package org.apache.hyracks.util;

import java.util.function.Consumer;

import com.google.common.util.concurrent.UncheckedExecutionException;

@FunctionalInterface
public interface ThrowingConsumer<V> {
    void process(V value) throws Exception;

    @SuppressWarnings("Duplicates")
    static <T> Consumer<T> asUnchecked(ThrowingConsumer<T> consumer) {
        return input -> {
            try {
                consumer.process(input);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException(e);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        };
    }

    static <R> ThrowingFunction<R, Void> asFunction(ThrowingConsumer<R> consumer) {
        return input -> {
            consumer.process(input);
            return null;
        };
    }
}
