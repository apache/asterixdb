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

package org.apache.asterix.experiment.client;

import org.kohsuke.args4j.Option;

public class SyntheticDataGeneratorConfig {

    @Option(name = "-psf", aliases = "--point-source-file", usage = "The point source file")
    private String pointSourceFile;

    public String getPointSourceFile() {
        return pointSourceFile;
    }
    
    @Option(name = "-psi", aliases = "--point-sampling-interval", usage = "The point sampling interval from the point source file", required = true)
    private int pointSamplingInterval;

    public int getpointSamplingInterval() {
        return pointSamplingInterval;
    }
    
    @Option(name = "-pid", aliases = "--parition-id", usage = "The partition id in order to avoid key duplication", required = true)
    private int partitionId;

    public int getPartitionId() {
        return partitionId;
    }
    
    @Option(name = "-of", aliases = "--output-filepath", usage = "The output file path", required = true)
    private String outputFilePath;

    public String getOutputFilePath() {
        return outputFilePath;
    }
    
    @Option(name = "-rc", aliases = "--record-count", usage = "The record count to generate", required = true)
    private long recordCount;

    public long getRecordCount() {
        return recordCount;
    }        
}
