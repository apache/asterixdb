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
package org.apache.asterix.translator;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.api.ISchedulableClientRequest;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.job.JobSpecification;

public class SchedulableClientRequest implements ISchedulableClientRequest {

    private final IClientRequest clientRequest;
    private final JobSpecification jobSpec;
    private final IMetadataProvider metadataProvider;
    private final ICommonRequestParameters requestParameters;

    private SchedulableClientRequest(IClientRequest clientRequest, ICommonRequestParameters requestParameters,
            IMetadataProvider metadataProvider, JobSpecification jobSpec) {
        this.clientRequest = clientRequest;
        this.requestParameters = requestParameters;
        this.metadataProvider = metadataProvider;
        this.jobSpec = jobSpec;
    }

    public static SchedulableClientRequest of(IClientRequest clientRequest, ICommonRequestParameters requestParameters,
            IMetadataProvider metadataProvider, JobSpecification jobSpec) {
        return new SchedulableClientRequest(clientRequest, requestParameters, metadataProvider, jobSpec);
    }

    @Override
    public IClientRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    public ICommonRequestParameters getRequestParameters() {
        return requestParameters;
    }

    @Override
    public JobSpecification getJobSpecification() {
        return jobSpec;
    }

    @Override
    public IMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }
}
