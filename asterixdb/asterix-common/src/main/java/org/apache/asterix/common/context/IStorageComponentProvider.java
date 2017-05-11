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
package org.apache.asterix.common.context;

import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.common.IStorageManager;

/**
 * Responsible for storage components
 */
public interface IStorageComponentProvider {

    /**
     * @return {@link org.apache.asterix.common.context.ITransactionSubsystemProvider} instance
     */
    ITransactionSubsystemProvider getTransactionSubsystemProvider();

    /**
     * @return {@link org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider} instance
     */
    ILSMIOOperationSchedulerProvider getIoOperationSchedulerProvider();

    /**
     * @return {@link org.apache.hyracks.storage.common.IStorageManager} instance
     */
    IStorageManager getStorageManager();

    /**
     * @return {@link org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory} instance
     */
    IMetadataPageManagerFactory getMetadataPageManagerFactory();

    /**
     * @return {@link org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory} instance
     */
    IPrimitiveValueProviderFactory getPrimitiveValueProviderFactory();

    /**
     * @return {@link org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider} instance
     */
    IBinaryComparatorFactoryProvider getComparatorFactoryProvider();

    /**
     * @return {@link org.apache.hyracks.algebricks.data.ITypeTraitProvider} instance
     */
    ITypeTraitProvider getTypeTraitProvider();
}
