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
package org.apache.hyracks.storage.am.lsm.btree.column.dataflow;

import java.util.Map;

import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.common.IStorageManager;

public class LSMColumnBTreeLocalResourceFactory extends LSMBTreeLocalResourceFactory {
    private static final long serialVersionUID = -676367767925618165L;
    private final IColumnManagerFactory columnManagerFactory;

    public LSMColumnBTreeLocalResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate,
            int[] btreeFields, ICompressorDecompressorFactory compressorDecompressorFactory, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector, boolean isSecondaryNoIncrementalMaintenance,
            IColumnManagerFactory columnManagerFactory, boolean atomic) {
        super(storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, true, bloomFilterKeyFields,
                bloomFilterFalsePositiveRate, true, btreeFields, compressorDecompressorFactory, true, nullTypeTraits,
                nullIntrospector, isSecondaryNoIncrementalMaintenance, atomic);
        this.columnManagerFactory = columnManagerFactory;
    }

    @Override
    public LsmResource createResource(FileReference fileRef) {
        return new LSMColumnBTreeLocalResource(typeTraits, cmpFactories, bloomFilterKeyFields,
                bloomFilterFalsePositiveRate, fileRef.getRelativePath(), storageManager, mergePolicyFactory,
                mergePolicyProperties, btreeFields, opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, compressorDecompressorFactory,
                nullTypeTraits, nullIntrospector, isSecondaryNoIncrementalMaintenance, columnManagerFactory, atomic);
    }
}
