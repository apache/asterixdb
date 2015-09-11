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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class IOCounterLinux implements IIOCounter {
    public static final String COMMAND = "iostat";
    public static final String STATFILE = "/proc/self/io";
    public static final int PAGE_SIZE = 4096;

    private long baseReads = 0;
    private long baseWrites = 0;

    public IOCounterLinux() {
        baseReads = getReads();
        baseWrites = getWrites();
    }

    @Override
    public long getReads() {
        try {
            long reads = extractRow(4);
            return reads;
        } catch (IOException e) {
            try {
                long reads = extractColumn(4) * PAGE_SIZE;
                return reads - baseReads;
            } catch (IOException e2) {
                return 0;
            }
        }
    }

    @Override
    public long getWrites() {
        try {
            long writes = extractRow(5);
            long cancelledWrites = extractRow(6);
            return (writes - cancelledWrites);
        } catch (IOException e) {
            try {
                long writes = extractColumn(5) * PAGE_SIZE;
                return writes - baseWrites;
            } catch (IOException e2) {
                return 0;
            }
        }
    }

    private long extractColumn(int columnIndex) throws IOException {
        BufferedReader reader = exec(COMMAND);
        String line = null;
        boolean device = false;
        long ios = 0;
        while ((line = reader.readLine()) != null) {
            if (line.contains("Blk_read")) {
                device = true;
                continue;
            }
            if (device == true) {
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
        reader.close();
        return ios;
    }

    private long extractRow(int rowIndex) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(STATFILE)));
        String line = null;
        long ios = 0;
        int i = 0;
        while ((line = reader.readLine()) != null) {
            if (i == rowIndex) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                int j = 0;
                while (tokenizer.hasMoreTokens()) {
                    String column = tokenizer.nextToken();
                    if (j == 1) {
                        ios = Long.parseLong(column);
                        break;
                    }
                    j++;
                }
            }
            i++;
        }
        reader.close();
        return ios;
    }

    private BufferedReader exec(String command) throws IOException {
        Process p = Runtime.getRuntime().exec(command);
        return new BufferedReader(new InputStreamReader(p.getInputStream()));
    }

}
