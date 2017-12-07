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
package org.apache.asterix.installer.test;

import java.io.File;

import org.apache.asterix.event.model.AsterixInstance.State;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AsterixLifecycleIT {

    private static final String PATH_ACTUAL = "target" + File.separator + "ittest" + File.separator;

    @BeforeClass
    public static void setUp() throws Exception {
        AsterixInstallerIntegrationUtil.init(AsterixInstallerIntegrationUtil.LOCAL_CLUSTER_PATH);
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }

    public static void restartInstance() throws Exception {
        AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.INACTIVE);
        AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
    }
}
