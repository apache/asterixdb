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
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class ExternalRTreeLocalResourceMetadataFactory extends LSMRTreeLocalResourceMetadataFactory {

    private static final long serialVersionUID = 1L;

    public ExternalRTreeLocalResourceMetadataFactory(ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int datasetID,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            int[] btreeFields, boolean isPointMBR) {
        super(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                linearizeCmpFactory, datasetID, mergePolicyFactory, mergePolicyProperties, null, null, null,
                btreeFields, null, isPointMBR);
    }

    @Override
    public Resource resource(int partition) {
        return new ExternalRTreeLocalResourceMetadata(filterTypeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, rtreePolicyType, linearizeCmpFactory, datasetId, partition, mergePolicyFactory,
                mergePolicyProperties, btreeFields, isPointMBR);
    }

}
