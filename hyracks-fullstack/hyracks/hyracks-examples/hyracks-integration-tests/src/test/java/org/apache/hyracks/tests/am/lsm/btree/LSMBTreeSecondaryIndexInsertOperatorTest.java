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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.tests.am.btree.BTreeSecondaryIndexInsertOperatorTest;
import org.apache.hyracks.tests.am.btree.DataSetConstants;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;

public class LSMBTreeSecondaryIndexInsertOperatorTest extends BTreeSecondaryIndexInsertOperatorTest {
    @Override
    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksDataException {
        return new LSMBTreeOperatorTestHelper(TestStorageManagerComponentHolder.getIOManager());
    }

    @Override
    protected IResourceFactory createPrimaryResourceFactory() {
        return ((LSMBTreeOperatorTestHelper) testHelper).getLocalResourceFactory(storageManager,
                DataSetConstants.primaryTypeTraits, DataSetConstants.primaryComparatorFactories,
                (IMetadataPageManagerFactory) pageManagerFactory, DataSetConstants.primaryBloomFilterKeyFields,
                DataSetConstants.primaryBtreeFields, DataSetConstants.primaryFilterFields,
                DataSetConstants.filterTypeTraits, DataSetConstants.filterCmpFactories);
    }

    @Override
    protected IResourceFactory createSecondaryResourceFactory() {
        return ((LSMBTreeOperatorTestHelper) testHelper).getLocalResourceFactory(storageManager,
                DataSetConstants.secondaryTypeTraits, DataSetConstants.secondaryComparatorFactories,
                (IMetadataPageManagerFactory) pageManagerFactory, DataSetConstants.secondaryBloomFilterKeyFields,
                DataSetConstants.secondaryBtreeFields, DataSetConstants.secondaryFilterFields,
                DataSetConstants.filterTypeTraits, DataSetConstants.filterCmpFactories);
    }

}
