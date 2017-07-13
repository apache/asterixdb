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

package org.apache.asterix.rebalance;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.api.client.IHyracksClientConnection;

// The callback performs no action before and after a rebalance.
public class NoOpDatasetRebalanceCallback implements IDatasetRebalanceCallback {

    public static final NoOpDatasetRebalanceCallback INSTANCE = new NoOpDatasetRebalanceCallback();

    private NoOpDatasetRebalanceCallback() {

    }

    @Override
    public void beforeRebalance(MetadataProvider metadataProvider, Dataset source, Dataset target,
            IHyracksClientConnection hcc) {
        // Does nothing.
    }

    @Override
    public void afterRebalance(MetadataProvider metadataProvider, Dataset source, Dataset target,
            IHyracksClientConnection hcc) {
        // Does nothing.
    }

}
