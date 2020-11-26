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
package org.apache.asterix.test.external_dataset.aws;

import java.util.Collection;

import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

/**
 * Runs an AWS S3 mock server and test it as an external dataset using one node one partition.
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AwsS3ExternalDatasetOnePartitionTest extends AwsS3ExternalDatasetTest {

    public AwsS3ExternalDatasetOnePartitionTest(TestCaseContext tcCtx) {
        super(tcCtx);
    }

    @Parameterized.Parameters(name = "AwsS3ExternalDatasetOnePartitionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        SUITE_TESTS = "testsuite_external_dataset_s3_one_partition.xml";
        ONLY_TESTS = "only_external_dataset.xml";
        TEST_CONFIG_FILE_NAME = "src/test/resources/cc-single.conf";
        PREPARE_BUCKET = AwsS3ExternalDatasetOnePartitionTest::prepareS3Bucket;
        PREPARE_FIXED_DATA_BUCKET = AwsS3ExternalDatasetOnePartitionTest::prepareFixedDataBucket;
        PREPARE_MIXED_DATA_BUCKET = AwsS3ExternalDatasetOnePartitionTest::prepareMixedDataBucket;
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    private static void prepareS3Bucket() {
    }

    private static void prepareFixedDataBucket() {
    }

    private static void prepareMixedDataBucket() {
    }
}
