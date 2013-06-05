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
package edu.uci.ics.asterix.installer.model;

import java.io.Serializable;
import java.util.List;

public class AsterixRuntimeState implements Serializable {

    private final List<ProcessInfo> processes;
    private final List<String> failedNCs;
    private final boolean ccRunning;
    private String summary;

    public AsterixRuntimeState(List<ProcessInfo> processes, List<String> failedNCs, boolean ccRunning) {
        this.processes = processes;
        this.failedNCs = failedNCs;
        this.ccRunning = ccRunning;
    }

    public List<ProcessInfo> getProcesses() {
        return processes;
    }

    public List<String> getFailedNCs() {
        return failedNCs;
    }

    public boolean isCcRunning() {
        return ccRunning;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getSummary() {
        return summary;
    }

}
