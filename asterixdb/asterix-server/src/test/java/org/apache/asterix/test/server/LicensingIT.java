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
package org.apache.asterix.test.server;

import java.io.IOException;

import org.apache.hyracks.test.support.LicensingTestBase;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class LicensingIT extends LicensingTestBase {

    @Before
    public void setup() {
        initInstallerDir();
    }

    @Override
    protected String getTargetDir() {
        return FileUtil.joinPath("target");
    }

    @Override
    protected String getInstallerDirPattern() {
        return "asterix-server.*-binary-assembly";
    }

    @Override
    protected String getTopLevelDirPattern() {
        return "apache-asterixdb.*";
    }

    @Override
    protected String pathToLicensingFiles() {
        return "";
    }

    @Override
    protected String[] getRequiredArtifactNames() {
        return org.apache.commons.lang3.ArrayUtils.add(getLicenseArtifactNames(), "NOTICE");
    }

    @Override
    protected String[] getLicenseArtifactNames() {
        return new String[] { "LICENSE" };
    }

    @Test
    public void testLicenseNoticeFilesPresent() {
        verifyAllRequiredArtifactsPresent();
    }

    @Test
    public void ensureNoMissingLicenses() throws IOException {
        verifyMissingLicenses();
    }

}
