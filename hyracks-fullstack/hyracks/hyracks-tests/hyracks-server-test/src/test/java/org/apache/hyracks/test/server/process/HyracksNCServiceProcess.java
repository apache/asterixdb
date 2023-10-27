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
package org.apache.hyracks.test.server.process;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.control.nc.service.NCService;

public class HyracksNCServiceProcess extends HyracksServerProcess {
    private static final AtomicInteger ncServiceCounter = new AtomicInteger();

    HyracksNCServiceProcess(File configFile, File logFile, File appHome, File workingDir) {
        super("nc" + ncServiceCounter.incrementAndGet());
        this.configFile = configFile;
        this.logFile = logFile;
        this.appHome = appHome;
        this.workingDir = workingDir;
    }

    @Override
    protected String getMainClassName() {
        return NCService.class.getName();
    }

    @Override
    protected void addJvmArgs(List<String> cList) {
        // NCService needs little memory
        args.add("-Xmx128m");
        cList.addAll(args);
    }

}
