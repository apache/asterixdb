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
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This interface is used for customizing the before/after operation for rebalance.
 */
public interface IDatasetRebalanceCallback {

    /**
     * The action to perform before the target dataset is populated.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @param source,
     *            the source dataset.
     * @param target,
     *            the target dataset.
     * @param hcc,
     *            the hyracks client connection.
     * @throws HyracksDataException
     */
    void beforeRebalance(MetadataProvider metadataProvider, Dataset source, Dataset target,
            IHyracksClientConnection hcc) throws HyracksDataException;

    /**
     * The action to perform after the target datasets is populated.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @param source,
     *            the source dataset.
     * @param target,
     *            the target dataset.
     * @param hcc,
     *            the hyracks client connection.
     * @throws HyracksDataException
     */
    void afterRebalance(MetadataProvider metadataProvider, Dataset source, Dataset target, IHyracksClientConnection hcc)
            throws HyracksDataException;

}
