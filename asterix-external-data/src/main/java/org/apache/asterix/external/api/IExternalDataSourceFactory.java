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
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;

public interface IExternalDataSourceFactory extends Serializable {

    /**
     * The data source type indicates whether the data source produces a continuous stream or
     * a set of records
     */
    public enum DataSourceType {
        STREAM,
        RECORDS
    }

    /**
     * @return The data source type {STREAM or RECORDS}
     */
    public DataSourceType getDataSourceType();

    /**
     * Specifies on which locations this data source is expected to run.
     *
     * @return
     * @throws AsterixException
     */
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AsterixException;

    /**
     * Configure the data parser factory. The passed map contains key value pairs from the
     * submitted AQL statement and any additional pairs added by the compiler
     *
     * @param configuration
     * @throws AsterixException
     */
    public void configure(Map<String, String> configuration) throws AsterixException;

    /**
     * Specify whether the external data source can be indexed
     *
     * @return
     */
    public default boolean isIndexible() {
        return false;
    }

    public static AlgebricksAbsolutePartitionConstraint getPartitionConstraints(
            AlgebricksAbsolutePartitionConstraint constraints, int count) {
        if (constraints == null) {
            ArrayList<String> locs = new ArrayList<String>();
            Map<String, String[]> stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
            int i = 0;
            while (i < count) {
                for (String node : stores.keySet()) {
                    int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(node);
                    for (int k = 0; k < numIODevices; k++) {
                        locs.add(node);
                        i++;
                        if (i == count) {
                            break;
                        }
                    }
                    if (i == count) {
                        break;
                    }
                }
            }
            String[] cluster = new String[locs.size()];
            cluster = locs.toArray(cluster);
            constraints = new AlgebricksAbsolutePartitionConstraint(cluster);
        }
        return constraints;
    }
}
