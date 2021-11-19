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

import java.util.function.BiConsumer;

import com.google.common.util.concurrent.UncheckedExecutionException;

@FunctionalInterface
public interface ThrowingBiConsumer<L, R> {
    void process(L left, R right) throws Exception;

    @SuppressWarnings("Duplicates")
    static <L, R> BiConsumer<L, R> asUnchecked(ThrowingBiConsumer<L, R> consumer) {
        return (left, right) -> {
            try {
                consumer.process(left, right);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException(e);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        };
    }

    static <I, J> ThrowingBiFunction<I, J, Void> asFunction(ThrowingBiConsumer<I, J> consumer) {
        return (p1, p2) -> {
            consumer.process(p1, p2);
            return null;
        };
    }
}
