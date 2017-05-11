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
package org.apache.asterix.metadata.api;

import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.common.IResourceFactory;

@FunctionalInterface
public interface IResourceFactoryProvider {

    /**
     * Get the index resource factory
     *
     * @param mdProvider
     *            the system's metadata provider
     * @param dataset
     *            the index dataset
     * @param index
     *            the index
     * @param recordType
     *            the dataset's record type
     * @param metaType
     *            the detaset's meta type
     * @param mergePolicyFactory
     *            the index's merge policy factory
     * @param mergePolicyProperties
     *            the index's merge policy properties
     * @param filterTypeTraits
     *            the dataset's filter type traits
     * @param filterCmpFactories
     *            the dataset's filter comparator factories
     * @return the index dataflow helper factory
     * @throws AlgebricksException
     *             if the dataflow helper factory couldn't be created for the index
     */
    IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException;
}
