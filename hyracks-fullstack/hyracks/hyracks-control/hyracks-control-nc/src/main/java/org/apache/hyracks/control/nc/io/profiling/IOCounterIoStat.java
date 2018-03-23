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

import static org.apache.hyracks.control.nc.io.profiling.IOCounterDefault.IO_COUNTER_UNAVAILABLE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IOCounterIoStat extends IOCounterCache<List<String>> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String COMMAND = "iostat";
    private static final int PAGE_SIZE = 512;
    private long failureCount;

    private long baseReads;
    private long baseWrites;

    IOCounterIoStat() {
        baseReads = getReads();
        baseWrites = getWrites();
    }

    @Override
    public long getReads() {
        try {
            long reads = extractColumn(4) * PAGE_SIZE;
            return reads == 0 ? IO_COUNTER_UNAVAILABLE : reads - baseReads;
        } catch (Exception e) {
            LOGGER.log(failureCount++ > 0 ? Level.DEBUG : Level.WARN, "Failure getting reads", e);
            return IO_COUNTER_UNAVAILABLE;
        }
    }

    @Override
    public long getWrites() {
        try {
            long writes = extractColumn(5) * PAGE_SIZE;
            return writes == 0 ? IO_COUNTER_UNAVAILABLE : writes - baseWrites;
        } catch (Exception e) {
            LOGGER.log(failureCount++ > 0 ? Level.DEBUG : Level.WARN, "Failure getting writes", e);
            return IO_COUNTER_UNAVAILABLE;
        }
    }

    private long extractColumn(int columnIndex) throws IOException {
        boolean device = false;
        long ios = 0;
        for (String line : getInfo()) {
            if (line.contains("Blk_read")) {
                device = true;
                continue;
            }
            if (device) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                int i = 0;
                while (tokenizer.hasMoreTokens()) {
                    String column = tokenizer.nextToken();
                    if (i == columnIndex) {
                        ios += Long.parseLong(column);
                        break;
                    }
                    i++;
                }
            }
        }
        return ios;
    }

    @Override
    protected List<String> calculateInfo() throws IOException {
        try (InputStream inputStream = Runtime.getRuntime().exec(COMMAND).getInputStream()) {
            return IOUtils.readLines(inputStream, Charset.defaultCharset());
        }
    }
}
