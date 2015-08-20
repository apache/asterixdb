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
package edu.uci.ics.hyracks.server.process;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class HyracksServerProcess {
    private static final Logger LOGGER = Logger.getLogger(HyracksServerProcess.class.getName());

    protected Process process;

    public void start() throws IOException {
        String[] cmd = buildCommand();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting command: " + Arrays.toString(cmd));
        }
        process = Runtime.getRuntime().exec(cmd, null, null);
        dump(process.getInputStream());
        dump(process.getErrorStream());
    }

    private void dump(InputStream input) {
        final int streamBufferSize = 1000;
        final Reader in = new InputStreamReader(input);
        new Thread(new Runnable() {
            public void run() {
                try {
                    char[] chars = new char[streamBufferSize];
                    int c;
                    while ((c = in.read(chars)) != -1) {
                        if (c > 0) {
                            System.out.print(String.valueOf(chars, 0, c));
                        }
                    }
                } catch (IOException e) {
                }
            }
        }).start();
    }

    private String[] buildCommand() {
        List<String> cList = new ArrayList<String>();
        cList.add(getJavaCommand());
        cList.add("-Dbasedir=" + System.getProperty("basedir"));
        cList.add("-Djava.rmi.server.hostname=127.0.0.1");
        cList.add("-classpath");
        cList.add(getClasspath());
        cList.add(getMainClassName());
        addCmdLineArgs(cList);
        return cList.toArray(new String[cList.size()]);
    }

    protected abstract void addCmdLineArgs(List<String> cList);

    protected abstract String getMainClassName();

    private final String getClasspath() {
        return System.getProperty("java.class.path");
    }

    protected final String getJavaCommand() {
        return System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    }
}