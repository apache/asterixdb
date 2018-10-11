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
package org.apache.asterix.external.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IExternalDataSourceFactory extends Serializable {

    /**
     * The data source type indicates whether the data source produces a continuous stream or
     * a set of records
     */
    enum DataSourceType {
        STREAM,
        RECORDS
    }

    /**
     * @return The data source type {STREAM or RECORDS}
     */
    DataSourceType getDataSourceType();

    /**
     * Specifies on which locations this data source is expected to run.
     *
     * @return
     * @throws AsterixException
     */
    AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException;

    /**
     * Configure the data parser factory. The passed map contains key value pairs from the
     * submitted AQL statement and any additional pairs added by the compiler
     *
     * @param configuration
     * @throws AsterixException
     */
    void configure(IServiceContext ctx, Map<String, String> configuration)
            throws AlgebricksException, HyracksDataException;

    /**
     * Specify whether the external data source can be indexed
     *
     * @return
     */
    default boolean isIndexible() {
        return false;
    }

    /**
     * returns the passed partition constraints if not null, otherwise returns round robin absolute partition
     * constraints that matches the count.
     *
     * @param constraints
     * @param count
     * @return
     * @throws AlgebricksException
     */
    static AlgebricksAbsolutePartitionConstraint getPartitionConstraints(ICcApplicationContext appCtx,
            AlgebricksAbsolutePartitionConstraint constraints, int count) throws AlgebricksException {
        if (constraints == null) {
            IClusterStateManager clusterStateManager = appCtx.getClusterStateManager();
            ArrayList<String> locs = new ArrayList<>();
            Set<String> stores = appCtx.getMetadataProperties().getStores().keySet();
            if (stores.isEmpty()) {
                throw new AlgebricksException("Configurations don't have any stores");
            }
            int i = 0;
            outer: while (i < count) {
                Iterator<String> storeIt = stores.iterator();
                while (storeIt.hasNext()) {
                    String node = storeIt.next();
                    int numIODevices = clusterStateManager.getIODevices(node).length;
                    for (int k = 0; k < numIODevices; k++) {
                        locs.add(node);
                        i++;
                        if (i == count) {
                            break outer;
                        }
                    }
                }
                if (i == 0) {
                    throw new AlgebricksException("All stores have 0 IO devices");
                }
            }
            return new AlgebricksAbsolutePartitionConstraint(locs.toArray(new String[locs.size()]));
        }
        return constraints;
    }
}
