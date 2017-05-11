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
package org.apache.asterix.file;

import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.PrimitiveValueProviderFactory;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.common.IStorageManager;

public class StorageComponentProvider implements IStorageComponentProvider {

    @Override
    public IPrimitiveValueProviderFactory getPrimitiveValueProviderFactory() {
        return PrimitiveValueProviderFactory.INSTANCE;
    }

    @Override
    public ITransactionSubsystemProvider getTransactionSubsystemProvider() {
        return TransactionSubsystemProvider.INSTANCE;
    }

    @Override
    public ILSMIOOperationSchedulerProvider getIoOperationSchedulerProvider() {
        return RuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

    @Override
    public IMetadataPageManagerFactory getMetadataPageManagerFactory() {
        return AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;
    }

    @Override
    public IBinaryComparatorFactoryProvider getComparatorFactoryProvider() {
        return BinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public TypeTraitProvider getTypeTraitProvider() {
        return TypeTraitProvider.INSTANCE;
    }

    @Override
    public IStorageManager getStorageManager() {
        return RuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

}
