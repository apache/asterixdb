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
package org.apache.asterix.app.translator;

import java.util.Map;

import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.ResultProperties;
import org.apache.hyracks.api.dataset.IHyracksDataset;

public class RequestParameters implements IRequestParameters {

    private final IHyracksDataset hdc;
    private final ResultProperties resultProperties;
    private final Stats stats;
    private final Map<String, String> optionalParameters;
    private final IStatementExecutor.ResultMetadata outMetadata;
    private final String clientContextId;

    public RequestParameters(IHyracksDataset hdc, ResultProperties resultProperties, Stats stats,
            IStatementExecutor.ResultMetadata outMetadata, String clientContextId,
            Map<String, String> optionalParameters) {
        this.hdc = hdc;
        this.resultProperties = resultProperties;
        this.stats = stats;
        this.outMetadata = outMetadata;
        this.clientContextId = clientContextId;
        this.optionalParameters = optionalParameters;
    }

    @Override
    public IHyracksDataset getHyracksDataset() {
        return hdc;
    }

    @Override
    public ResultProperties getResultProperties() {
        return resultProperties;
    }

    @Override
    public IStatementExecutor.Stats getStats() {
        return stats;
    }

    @Override
    public Map<String, String> getOptionalParameters() {
        return optionalParameters;
    }

    @Override
    public IStatementExecutor.ResultMetadata getOutMetadata() {
        return outMetadata;
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
    }
}
