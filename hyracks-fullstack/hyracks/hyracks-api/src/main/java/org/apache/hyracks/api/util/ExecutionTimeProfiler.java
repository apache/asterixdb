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
import java.util.HashMap;
import java.util.LinkedHashMap;

public class ExecutionTimeProfiler {

    public static final boolean PROFILE_MODE = false;
    private FileOutputStream fos;
    private String filePath;
    private StringBuilder sb;
    private Object lock1 = new Object();

    // [Key: Job, Value: [Key: Operator, Value: Duration of each operators]]
    private HashMap<String, LinkedHashMap<String, String>> spentTimePerJobMap;

    public ExecutionTimeProfiler(String filePath) {
        this.filePath = new String(filePath);
        this.sb = new StringBuilder();
        this.spentTimePerJobMap = new HashMap<String, LinkedHashMap<String, String>>();
    }

    public void begin() {
        try {
            fos = ExperimentProfilerUtils.openOutputFile(filePath);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public synchronized void add(String jobSignature, String operatorSignature, String message, boolean flushNeeded) {

        if (!spentTimePerJobMap.containsKey(jobSignature)) {
            spentTimePerJobMap.put(jobSignature, new LinkedHashMap<String, String>());
        }
        spentTimePerJobMap.get(jobSignature).put(operatorSignature, message);

        if (flushNeeded) {
            flush(jobSignature);
        }
    }

    public synchronized void flush(String jobSignature) {
        try {
            synchronized (lock1) {
                sb.append("\n\n");
                spentTimePerJobMap.get(jobSignature).forEach((key, value) -> sb.append(value));
                fos.write(sb.toString().getBytes());
                fos.flush();
                spentTimePerJobMap.get(jobSignature).clear();
                sb.setLength(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public void clear() {
        spentTimePerJobMap.clear();
        sb.setLength(0);
    }

    public void clear(String jobSignature) {
        spentTimePerJobMap.get(jobSignature).clear();
        sb.setLength(0);
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
