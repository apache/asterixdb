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

package org.apache.hyracks.control.nc.io.profiling;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;

abstract class IOCounterCache<T> implements IIOCounter {
    private static final long TTL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private Span span;
    private T info;

    protected synchronized T getInfo() throws IOException {
        if (info == null || span.elapsed()) {
            span = Span.start(TTL_NANOS, TimeUnit.NANOSECONDS);
            info = calculateInfo();
        }
        return info;
    }

    protected abstract T calculateInfo() throws IOException;
}
