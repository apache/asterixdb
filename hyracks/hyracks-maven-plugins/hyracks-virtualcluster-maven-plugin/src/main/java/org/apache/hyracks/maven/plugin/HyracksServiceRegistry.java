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
package edu.uci.ics.hyracks.maven.plugin;

import java.util.ArrayList;
import java.util.List;

public class HyracksServiceRegistry {
    public static HyracksServiceRegistry INSTANCE = new HyracksServiceRegistry();

    private final List<Process> serviceProcesses;

    private HyracksServiceRegistry() {
        serviceProcesses = new ArrayList<Process>();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                destroyAll();
            }
        });
    }

    public synchronized void addServiceProcess(Process process) {
        serviceProcesses.add(process);
    }

    public synchronized void destroyAll() {
        for (Process p : serviceProcesses) {
            try {
                p.destroy();
            } catch (Exception e) {
            }
        }
        serviceProcesses.clear();
    }
}