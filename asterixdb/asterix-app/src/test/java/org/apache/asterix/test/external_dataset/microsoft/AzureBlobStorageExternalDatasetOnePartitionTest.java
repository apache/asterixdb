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
package org.apache.asterix.test.external_dataset.microsoft;

import java.util.Collection;

import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

@Ignore
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureBlobStorageExternalDatasetOnePartitionTest extends AzureBlobStorageExternalDatasetTest {

    public AzureBlobStorageExternalDatasetOnePartitionTest(TestCaseContext tcCtx) {
        super(tcCtx);
    }

    @Parameterized.Parameters(name = "AwsS3ExternalDatasetOnePartitionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        SUITE_TESTS = "testsuite_external_dataset_azure_blob_storage_one_partition.xml";
        ONLY_TESTS = "only_external_dataset.xml";
        TEST_CONFIG_FILE_NAME = "src/test/resources/cc-single.conf";
        PREPARE_PLAYGROUND_CONTAINER = AzureBlobStorageExternalDatasetOnePartitionTest::preparePlaygroundContainer;
        PREPARE_FIXED_DATA_CONTAINER = AzureBlobStorageExternalDatasetOnePartitionTest::prepareFixedDataContainer;
        PREPARE_MIXED_DATA_CONTAINER = AzureBlobStorageExternalDatasetOnePartitionTest::prepareMixedDataContainer;
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    private static void preparePlaygroundContainer() {
    }

    private static void prepareFixedDataContainer() {
    }

    private static void prepareMixedDataContainer() {
    }
}
