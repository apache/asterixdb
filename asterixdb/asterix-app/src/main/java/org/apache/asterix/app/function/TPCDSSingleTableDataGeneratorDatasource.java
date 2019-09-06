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

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

/**
 * This TPC-DS function is used to generate data with accordance to the specifications of the TPC Benchmark DS.
 */

public class TPCDSSingleTableDataGeneratorDatasource extends FunctionDataSource {

    private final String tableName;
    private final double scalingFactor;
    private final FunctionIdentifier functionIdentifier;

    TPCDSSingleTableDataGeneratorDatasource(INodeDomain domain, String tableName, double scalingFactor,
            FunctionIdentifier functionIdentifier) throws AlgebricksException {
        super(createDataSourceId(tableName, scalingFactor), domain);
        this.tableName = tableName;
        this.scalingFactor = scalingFactor;
        this.functionIdentifier = functionIdentifier;
    }

    /**
     * This ensures that each function will have a unique DataSourceId by passing the table name as part of the
     * DataSourceId. This eliminates the issue of creating a single function even though multiple functions calls
     * are happening with different parameters and the optimizer understands them as a single function.
     *
     * @param tableName
     *            table name to be added as part of the DataSourceId
     * @param scalingFactor
     *            scaling factor to be added as part of the DataSourceId
     * @return A DataSourceId that's based on the function details and its parameters
     */
    private static DataSourceId createDataSourceId(String tableName, double scalingFactor) {
        return createDataSourceId(TPCDSSingleTableDataGeneratorRewriter.TPCDS_SINGLE_TABLE_DATA_GENERATOR, tableName,
                Double.toString(scalingFactor));
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        return new TPCDSDataGeneratorFunction(locations, tableName, scalingFactor, functionIdentifier);
    }

    @Override
    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm) {
        return new AlgebricksAbsolutePartitionConstraint(csm.getClusterLocations().getLocations());
    }
}
