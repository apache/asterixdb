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
package org.apache.asterix.app.function;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This TPC-DS function is used to generate data with accordance to the specifications of the TPC Benchmark DS.
 */

public class TPCDSDataGeneratorFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final double scalingFactor;
    private final int parallelism;
    private final FunctionIdentifier functionIdentifier;

    TPCDSDataGeneratorFunction(AlgebricksAbsolutePartitionConstraint locations, String tableName, double scalingFactor,
            FunctionIdentifier functionIdentifier) {
        super(locations);
        this.tableName = tableName;
        this.scalingFactor = scalingFactor;
        this.functionIdentifier = functionIdentifier;

        /*
        TPC-DS has the option to parallelize the data generation and produce the data as chunks. We'll match the
        parallelism with the number of partitions we have, and assign each partition to take care of a certain chunk
         */
        this.parallelism = locations.getLocations().length;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return new TPCDSDataGeneratorReader(tableName, scalingFactor, parallelism, partition, getFunctionIdentifier());
    }

    /**
     * Gets the function identifier
     *
     * @return function identifier
     */
    private FunctionIdentifier getFunctionIdentifier() {
        return functionIdentifier;
    }
}
