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
package org.apache.hyracks.algebricks.runtime.base;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class AlgebricksPipeline implements Serializable {

    private static final long serialVersionUID = 1L;
    private final IPushRuntimeFactory[] runtimeFactories;
    private final RecordDescriptor[] recordDescriptors;
    private final IPushRuntimeFactory[] outputRuntimeFactories;
    private final int[] outputPositions;

    public AlgebricksPipeline(IPushRuntimeFactory[] runtimeFactories, RecordDescriptor[] recordDescriptors,
            IPushRuntimeFactory[] outputRuntimeFactories, int[] outputPositions) {
        this.runtimeFactories = runtimeFactories;
        this.recordDescriptors = recordDescriptors;
        this.outputRuntimeFactories = outputRuntimeFactories;
        this.outputPositions = outputPositions;
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

    public IPushRuntimeFactory[] getOutputRuntimeFactories() {
        return outputRuntimeFactories;
    }

    public int[] getOutputPositions() {
        return outputPositions;
    }

    // public int[] getProjectedColumns() {
    // return projectedColumns;
    // }
}
