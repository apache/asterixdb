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

package org.apache.hyracks.util.trace;

public interface ITraceCategoryRegistry {

    int NO_CATEGORIES = 64;

    long CATEGORIES_ALL = -1L;
    long CATEGORIES_NONE = 0L;

    ITraceCategoryRegistry NONE = new TraceCategoryRegistry() {
        @Override
        public long get(String name) {
            return CATEGORIES_NONE;
        }

        @Override
        public long get(String... names) {
            return CATEGORIES_NONE;
        }

        @Override
        public String getName(long categoryCode) {
            return "";
        }
    };

    long get(String name);

    long get(String... names);

    String getName(long categoryCode);
}
