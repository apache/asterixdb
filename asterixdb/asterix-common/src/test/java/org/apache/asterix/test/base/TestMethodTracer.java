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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/*
 * Traces method entry/exit to Log4j2 at org.apache.logging.log4j.Level.WARN (or supplied) level.  To use,
 * add the following to your test class:
 *
 * <code>
 *   @Rule
 *   public TestRule watcher = new TestMethodTracer();
 *
 *   @Rule
 *   public TestRule watcher = new TestMethodTracer(Level.INFO);
 * </code>
 */

public class TestMethodTracer extends TestWatcher {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Level level;

    public TestMethodTracer(Level level) {
        this.level = level;
    }

    public TestMethodTracer() {
        this(Level.WARN);
    }

    @Override
    protected void starting(Description description) {
        LOGGER.log(level, "### {} START", description.getMethodName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
        LOGGER.log(level, "### {} FAILED", description.getMethodName(), e);
    }

    @Override
    protected void succeeded(Description description) {
        LOGGER.log(level, "### {} SUCCEEDED", description.getMethodName());
    }
}
