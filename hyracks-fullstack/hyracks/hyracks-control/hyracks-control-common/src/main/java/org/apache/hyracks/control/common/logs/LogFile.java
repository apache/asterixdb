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
package org.apache.hyracks.control.common.logs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class LogFile {
    private final File root;

    private long openTime;
    private PrintWriter out;

    public LogFile(File root) {
        this.root = root;
    }

    public void open() throws Exception {
        openTime = System.currentTimeMillis();
    }

    public synchronized void log(ObjectNode object) throws Exception {
        if (out == null) {
            root.mkdirs();
            out = new PrintWriter(new FileOutputStream(new File(root, openTime + ".log"), true));
        }
        out.println(object.asText());
        out.flush();
    }

    public synchronized void close() {
        if (out != null) {
            out.flush();
            out.close();
            out = null;
        }
    }
}
