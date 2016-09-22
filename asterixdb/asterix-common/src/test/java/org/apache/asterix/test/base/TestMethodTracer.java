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
package org.apache.asterix.test.base;

import java.io.PrintStream;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/*
 * Traces method entry/exit to System.out (or supplied PrintStream).  To use, add the following to your test class:
 *
 *   @Rule
 *   public TestRule watcher = new TestMethodTracer();
 *
 *   @Rule
 *   public TestRule watcher = new TestMethodTracer(System.err);
 */

public class TestMethodTracer extends TestWatcher {

    private final PrintStream out;

    public TestMethodTracer(PrintStream out) {
        this.out = out;
    }

    public TestMethodTracer() {
        this(System.out);
    }

    @Override
    protected void starting(Description description) {
        out.println("## " + description.getMethodName() + " START");
    }

    @Override
    protected void failed(Throwable e, Description description) {
        out.println("## " + description.getMethodName() + " FAILED (" + e.getClass().getName() + ")");
    }

    @Override
    protected void succeeded(Description description) {
        out.println("## " + description.getMethodName() + " SUCCEEDED");
    }
}
