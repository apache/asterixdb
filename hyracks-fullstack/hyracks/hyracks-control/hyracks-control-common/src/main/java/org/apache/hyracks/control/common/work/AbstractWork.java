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
package org.apache.hyracks.control.common.work;

import org.apache.logging.log4j.Level;

public abstract class AbstractWork implements Runnable {

    public Level logLevel() {
        return Level.DEBUG;
    }

    public String getName() {
        final String className = getClass().getName();
        final int endIndex = className.endsWith("Work") ? className.length() - 4 : className.length();
        return className.substring(className.lastIndexOf('.') + 1, endIndex);
    }

    /**
     * run is executed on a single thread that services the work queue. As a result run should never wait or block as
     * this will delay processing for the whole queue.
     */
    public abstract void run();

    @Override
    public String toString() {
        return getName();
    }
}
