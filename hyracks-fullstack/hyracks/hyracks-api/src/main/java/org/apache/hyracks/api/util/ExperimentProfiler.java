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

import java.io.FileOutputStream;
import java.io.IOException;

public class ExperimentProfiler {

    public static final boolean PROFILE_MODE = false;
    private FileOutputStream fos;
    private String filePath;
    private StringBuilder sb;
    private int printInterval;
    private int addCount;

    public ExperimentProfiler(String filePath, int printInterval) {
        this.filePath = new String(filePath);
        this.sb = new StringBuilder();
        this.printInterval = printInterval;
    }

    public void begin() {
        try {
            fos = ExperimentProfilerUtils.openOutputFile(filePath);
            addCount = 0;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public synchronized void add(String s) {
        sb.append(s);
        if (printInterval > 0 && ++addCount % printInterval == 0) {
            flush();
            addCount = 0;
        }
    }

    public synchronized void flush() {
        try {
            fos.write(sb.toString().getBytes());
            fos.flush();
            sb.setLength(0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public synchronized void end() {
        try {
            if (fos != null) {
                fos.flush();
                fos.close();
                fos = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }
}
