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
import org.apache.asterix.common.transactions.ResourceFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class ExternalBTreeWithBuddyLocalResourceMetadataFactory extends ResourceFactory {
    private static final long serialVersionUID = 1L;
    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;
    private final int[] buddyBtreeFields;

    public ExternalBTreeWithBuddyLocalResourceMetadataFactory(int datasetID,
            IBinaryComparatorFactory[] btreeCmpFactories,
            ITypeTraits[] typeTraits, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] buddyBtreeFields) {
        super(datasetID, null, null, null);
        this.btreeCmpFactories = btreeCmpFactories;
        this.typeTraits = typeTraits;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.buddyBtreeFields = buddyBtreeFields;
    }

    @Override
    public Resource resource(int partition) {
        return new ExternalBTreeWithBuddyLocalResourceMetadata(datasetId, partition, btreeCmpFactories, typeTraits,
                mergePolicyFactory, mergePolicyProperties, buddyBtreeFields);
    }
}
