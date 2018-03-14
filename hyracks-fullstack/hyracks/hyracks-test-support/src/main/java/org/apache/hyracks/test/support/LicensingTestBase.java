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
package org.apache.hyracks.test.support;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.Assert;

public abstract class LicensingTestBase {
    private String installerDir;

    protected void initInstallerDir() {
        if (installerDir == null) {
            final String pattern = getInstallerDirPattern();
            final String targetDir = getTargetDir();
            final String[] list = new File(targetDir).list((dir, name) -> name.matches(pattern));
            Assert.assertNotNull("installerDir", list);
            Assert.assertFalse("Ambiguous install dir (" + pattern + "): " + Arrays.toString(list), list.length > 1);
            Assert.assertEquals("Can't find install dir (" + pattern + ")", 1, list.length);
            installerDir = FileUtil.joinPath(targetDir, list[0]);
        }
    }

    protected abstract String getTargetDir();

    protected abstract String getInstallerDirPattern();

    protected abstract String pathToLicensingFiles();

    protected abstract String[] getRequiredArtifactNames();

    protected void verifyMissingLicenses() throws IOException {
        for (String licenseArtifactName : getLicenseArtifactNames()) {
            final File licenseFile =
                    new File(FileUtil.joinPath(installerDir, pathToLicensingFiles(), licenseArtifactName));
            List<String> badLines = new ArrayList<>();
            for (String line : FileUtils.readLines(licenseFile, StandardCharsets.UTF_8)) {
                if (line.matches("^\\s*MISSING:.*")) {
                    badLines.add(line.trim());
                }
            }
            Assert.assertEquals("Missing licenses in " + licenseFile + ": " + badLines, 0, badLines.size());
        }
    }

    protected void verifyAllRequiredArtifactsPresent() {
        for (String name : getRequiredArtifactNames()) {
            final String fileName = FileUtil.joinPath(installerDir, pathToLicensingFiles(), name);
            Assert.assertTrue(fileName + " missing", new File(fileName).exists());
        }
    }

    protected abstract String[] getLicenseArtifactNames();
}
