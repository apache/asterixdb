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
package org.apache.asterix.transaction.management.resource;

import java.util.Map;

import org.apache.asterix.common.transactions.Resource;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class ExternalBTreeLocalResourceMetadataFactory extends LSMBTreeLocalResourceMetadataFactory {

    private static final long serialVersionUID = 1L;

    public ExternalBTreeLocalResourceMetadataFactory(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int datasetID,
            ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties) {
        super(typeTraits, cmpFactories, bloomFilterKeyFields, isPrimary, datasetID, mergePolicyFactory,
                mergePolicyProperties, null, null, null, null);
    }

    @Override
    public Resource resource(int partition) {
        return new ExternalBTreeLocalResourceMetadata(filterTypeTraits, filterCmpFactories, bloomFilterKeyFields,
                isPrimary, datasetId, partition, mergePolicyFactory, mergePolicyProperties);
    }
}
