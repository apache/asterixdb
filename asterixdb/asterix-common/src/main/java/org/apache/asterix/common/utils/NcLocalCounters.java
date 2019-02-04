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
package org.apache.asterix.common.utils;

import java.io.Serializable;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.NodeControllerService;

public class NcLocalCounters implements Serializable {
    private static final long serialVersionUID = 3798954558299915995L;

    private final long maxResourceId;
    private final long maxTxnId;
    private final long maxJobId;

    private NcLocalCounters(long maxResourceId, long maxTxnId, long maxJobId) {
        this.maxResourceId = maxResourceId;
        this.maxTxnId = maxTxnId;
        this.maxJobId = maxJobId;
    }

    public static NcLocalCounters collect(CcId ccId, NodeControllerService ncs) throws HyracksDataException {
        final INcApplicationContext appContext = (INcApplicationContext) ncs.getApplicationContext();
        long maxResourceId = Math.max(appContext.getLocalResourceRepository().maxId(),
                MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID);
        long maxTxnId = appContext.getMaxTxnId();
        long maxJobId = ncs.getMaxJobId(ccId);
        return new NcLocalCounters(maxResourceId, maxTxnId, maxJobId);
    }

    public long getMaxResourceId() {
        return maxResourceId;
    }

    public long getMaxTxnId() {
        return maxTxnId;
    }

    public long getMaxJobId() {
        return maxJobId;
    }
}
