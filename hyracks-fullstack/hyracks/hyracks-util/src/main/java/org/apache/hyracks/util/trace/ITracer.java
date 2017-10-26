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

public interface ITracer {

    enum Phase {
        // Duration Events
        B, // begin
        E, // end
        // Complete Events
        X,
        // Instant Events
        i,
        // Counter Events
        C,
        // Async Events
        b, // nestable start
        n, // nestable instant
        e, // nestable end
        // Flow Events
        s, // start
        t, // step
        f, // end
        // Object Events
        N, // created
        O, // snapshot
        D // destroyed
    }

    enum Scope {
        g, // Global scope
        p, // Process scope
        t // Thread scope
    }

    ITracer NONE = new ITracer() {
        @Override
        public String getName() {
            return "NONE";
        }

        @Override
        public ITraceCategoryRegistry getRegistry() {
            return ITraceCategoryRegistry.NONE;
        }

        @Override
        public void setCategories(String... categories) {
            // nothing to do here
        }

        @Override
        public boolean isEnabled(long cat) {
            return false;
        }

        @Override
        public long durationB(String name, long cat, String args) {
            return -1;
        }

        @Override
        public void durationE(String name, long cat, long tid, String args) {
            // nothing to do here
        }

        @Override
        public void durationE(long tid, long cat, String args) {
            // nothing to do here
        }

        @Override
        public void instant(String name, long cat, Scope scope, String args) {
            // nothing to do here
        }
    };

    String getName();

    ITraceCategoryRegistry getRegistry();

    void setCategories(String... categories);

    boolean isEnabled(long cat);

    long durationB(String name, long cat, String args);

    void durationE(long tid, long cat, String args);

    void durationE(String name, long cat, long tid, String args);

    void instant(String name, long cat, Scope scope, String args);

    @Override
    String toString();
}
