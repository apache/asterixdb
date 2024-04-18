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
package org.apache.hyracks.cloud.filesystem;

import java.io.File;

import javax.swing.filechooser.FileSystemView;

import org.junit.Test;

public class PhysicalDriveTest {

    @Test
    public void test() {
        FileSystemView fileSystemView = FileSystemView.getFileSystemView();
        File[] roots = File.listRoots();
        File file = new File(".");

        for (File root : roots) {
            if (file.getAbsolutePath().startsWith(root.getAbsolutePath())) {
                System.err.println(root.getAbsolutePath() + " is drive: " + fileSystemView.getSystemDisplayName(root));
            }
        }
    }
}
