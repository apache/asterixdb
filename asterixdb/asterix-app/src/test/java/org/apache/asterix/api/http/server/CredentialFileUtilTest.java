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
package org.apache.asterix.api.http.server;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CredentialFileUtilTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void missingFileIsFatalWhenRequired() {
        String path = new File(tmp.getRoot(), "passwd").getPath();
        IllegalStateException e =
                Assert.assertThrows(IllegalStateException.class, () -> CredentialFileUtil.load(path, true));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(path));
    }

    @Test
    public void missingFileIsIgnoredWhenNotRequired() {
        Assert.assertTrue(CredentialFileUtil.load(new File(tmp.getRoot(), "passwd").getPath(), false).isEmpty());
    }

    @Test
    public void directoryIsNotACredentialFile() {
        Assert.assertThrows(IllegalStateException.class, () -> CredentialFileUtil.load(tmp.getRoot().getPath(), true));
    }

    @Test
    public void malformedFileIsFatal() throws Exception {
        File passwd = write("admin:hash:extra\n");
        Assert.assertThrows(IllegalStateException.class, () -> CredentialFileUtil.load(passwd.getPath(), false));
    }

    @Test
    public void validFileIsLoaded() throws Exception {
        File passwd = write("admin:$2a$04$abc\nuser:$2a$04$def\n");
        Map<String, String> creds = CredentialFileUtil.load(passwd.getPath(), true);
        Assert.assertEquals(2, creds.size());
        Assert.assertEquals("$2a$04$abc", creds.get("admin"));
        Assert.assertEquals("$2a$04$def", creds.get("user"));
    }

    private File write(String content) throws Exception {
        File passwd = tmp.newFile("passwd");
        Files.write(passwd.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return passwd;
    }
}
