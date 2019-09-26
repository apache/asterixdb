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

package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class NoOpIndexCursorStats implements IIndexCursorStats {

    public static final IIndexCursorStats INSTANCE = new NoOpIndexCursorStats();

    private static final ICounter NOOP_COUNTER = new ICounter() {
        private static final long serialVersionUID = 1L;

        @Override
        public long update(long delta) {
            return 0;
        }

        @Override
        public long set(long value) {
            return 0;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public long get() {
            return 0;
        }
    };

    private NoOpIndexCursorStats() {
    }

    @Override
    public ICounter getPageCounter() {
        return NOOP_COUNTER;
    }

}
