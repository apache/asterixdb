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

package org.apache.hyracks.tests.am.lsm.btree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.tests.am.common.LSMTreeOperatorTestHelper;

public class LSMBTreeOperatorTestHelper extends LSMTreeOperatorTestHelper {

    public LSMBTreeOperatorTestHelper(IOManager ioManager) {
        super(ioManager);
    }

    public IResourceFactory getLocalResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, IMetadataPageManagerFactory pageManagerFactory,
            int[] bloomFilterKeyFields, int[] btreefields, int[] filterfields, ITypeTraits[] filtertypetraits,
            IBinaryComparatorFactory[] filtercmpfactories) {
        return new LSMBTreeLocalResourceFactory(storageManager, typeTraits, comparatorFactories, filtertypetraits,
                filtercmpfactories, filterfields, ThreadCountingOperationTrackerFactory.INSTANCE,
                NoOpIOOperationCallbackFactory.INSTANCE, NoOpPageWriteCallbackFactory.INSTANCE, pageManagerFactory,
                getVirtualBufferCacheProvider(), SynchronousSchedulerProvider.INSTANCE, MERGE_POLICY_FACTORY,
                MERGE_POLICY_PROPERTIES, DURABLE, bloomFilterKeyFields,
                LSMTreeOperatorTestHelper.DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, true, btreefields,
                NoOpCompressorDecompressorFactory.INSTANCE);
    }
}
