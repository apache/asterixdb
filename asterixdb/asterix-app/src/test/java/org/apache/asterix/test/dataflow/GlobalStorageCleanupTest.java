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
package org.apache.asterix.test.dataflow;

import java.io.File;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GlobalStorageCleanupTest {

    public static final Logger LOGGER = LogManager.getLogger();
    private static TestNodeController nc;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc.conf";
        nc = new TestNodeController(configPath, false);
    }

    @Test
    public void globalStorageCleanup() throws Exception {
        nc.init(true);
        LSMFlushRecoveryTest.nc = nc;
        LSMFlushRecoveryTest lsmFlushRecoveryTest = new LSMFlushRecoveryTest();
        lsmFlushRecoveryTest.initializeTestCtx();
        lsmFlushRecoveryTest.createIndex();
        lsmFlushRecoveryTest.readIndex();
        nc.deInit(false);
        nc.init(false);
        // the index should deleted after the node initialization
        lsmFlushRecoveryTest.initializeTestCtx();
        boolean failedToReadIndex = false;
        try {
            lsmFlushRecoveryTest.readIndex();
        } catch (Exception e) {
            failedToReadIndex = true;
            Assert.assertTrue(e.getMessage().contains(ErrorCode.INDEX_DOES_NOT_EXIST.errorCode()));
        }
        Assert.assertTrue(failedToReadIndex);
        nc.deInit(false);
    }
}
