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

import org.apache.asterix.app.message.ClientRequestsRequest;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class CompletedRequestsDatasource extends FunctionDataSource {

    private static final DataSourceId COMPLETED_REQUESTS_DATASOURCE_ID =
            createDataSourceId(CompletedRequestsRewriter.COMPLETED_REQUESTS);

    public CompletedRequestsDatasource(INodeDomain domain) throws AlgebricksException {
        super(COMPLETED_REQUESTS_DATASOURCE_ID, CompletedRequestsRewriter.COMPLETED_REQUESTS, domain);
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        AlgebricksAbsolutePartitionConstraint randomLocation =
                AlgebricksAbsolutePartitionConstraint.randomLocation(locations.getLocations());
        return new ClientRequestsFunction(randomLocation, ClientRequestsRequest.RequestType.COMPLETED);
    }

    @Override
    public boolean skipJobCapacityAssignment() {
        return true;
    }
}
