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

public class StopWatch {
    private long startTime = 0;
    private long stopTime = 0;
    private long elapsedTime = 0;

    public void start() {
        elapsedTime = 0;
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        stopTime = System.currentTimeMillis();
        elapsedTime += stopTime - startTime;
    }

    public void resume() {
        startTime = System.currentTimeMillis();
    }

    //elaspsed time in milliseconds
    public long getElapsedTime() {
        return elapsedTime;
    }

    //elaspsed time in seconds
    public long getElapsedTimeSecs() {
        return elapsedTime / 1000;
    }
}