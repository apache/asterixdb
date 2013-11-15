/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.control.nc.io.profiling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class IOCounterLinux implements IIOCounter {
    public static final String COMMAND = "iostat";
    public static final String COMMAND2 = "cat /proc/self/io";
    public static final int PAGE_SIZE = 4096;

    @Override
    public long getReads() {
        try {
            long reads = extractColumn(4);
            return reads;
        } catch (IOException e) {
            try {
                long reads = extractRow(4);
                return reads / PAGE_SIZE;
            } catch (IOException e2) {
                return 0;
            }
        }
    }

    @Override
    public long getWrites() {
        try {
            long writes = extractColumn(5);
            return writes;
        } catch (IOException e) {
            try {
                long writes = extractRow(5);
                long cancelledWrites = extractRow(6);
                return (writes - cancelledWrites) / PAGE_SIZE;
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
                    }
                    i++;
                }
            }
        }
        reader.close();
        return ios;
    }

    private long extractRow(int rowIndex) throws IOException {
        BufferedReader reader = exec(COMMAND2);
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
