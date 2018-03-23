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

package org.apache.hyracks.control.nc.io.profiling;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IOCounterProc extends IOCounterCache<List<String>> {
    private static final Logger LOGGER = LogManager.getLogger();
    @SuppressWarnings("squid:S1075") // hardcoded URI
    public static final File STATFILE = new File("/proc/self/io");
    private long failureCount;

    @Override
    public long getReads() {
        try {
            return extractRow(getInfo(), 4);
        } catch (Exception e) {
            LOGGER.log(failureCount++ > 0 ? Level.DEBUG : Level.WARN, "Failure getting reads", e);
            return IOCounterDefault.IO_COUNTER_UNAVAILABLE;
        }
    }

    @Override
    public long getWrites() {
        try {
            List<String> rows = getInfo();
            long writes = extractRow(rows, 5);
            long cancelledWrites = extractRow(rows, 6);
            return writes - cancelledWrites;
        } catch (Exception e) {
            LOGGER.log(failureCount++ > 0 ? Level.DEBUG : Level.WARN, "Failure getting writes", e);
            return IOCounterDefault.IO_COUNTER_UNAVAILABLE;
        }
    }

    private long extractRow(List<String> rows, int rowIndex) {
        return Long.parseLong(StringUtils.split(rows.get(rowIndex), ' ')[1]);
    }

    @Override
    protected List<String> calculateInfo() throws IOException {
        return FileUtils.readLines(STATFILE, Charset.defaultCharset());
    }

}
