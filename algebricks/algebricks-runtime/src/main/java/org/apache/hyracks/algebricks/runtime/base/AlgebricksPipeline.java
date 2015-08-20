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
package edu.uci.ics.hyracks.algebricks.runtime.base;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class AlgebricksPipeline implements Serializable {

    private static final long serialVersionUID = 1L;
    private final IPushRuntimeFactory[] runtimeFactories;
    private final RecordDescriptor[] recordDescriptors;

    public AlgebricksPipeline(IPushRuntimeFactory[] runtimeFactories, RecordDescriptor[] recordDescriptors) {
        this.runtimeFactories = runtimeFactories;
        this.recordDescriptors = recordDescriptors;
        // this.projectedColumns = projectedColumns;
    }

    public IPushRuntimeFactory[] getRuntimeFactories() {
        return runtimeFactories;
    }

    public RecordDescriptor[] getRecordDescriptors() {
        return recordDescriptors;
    }

    public int getOutputWidth() {
        return recordDescriptors[recordDescriptors.length - 1].getFieldCount();
    }

    // public int[] getProjectedColumns() {
    // return projectedColumns;
    // }

}