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

package org.apache.hyracks.storage.am.lsm.common.component;

import java.io.FilenameFilter;
import java.util.ArrayList;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

public class TestLsmIndexFileManager extends AbstractLSMIndexFileManager {

    public TestLsmIndexFileManager(IIOManager ioManager, FileReference file) {
        super(ioManager, file, null);
    }

    @Override
    protected void cleanupAndGetValidFilesInternal(FilenameFilter filter,
            TreeIndexFactory<? extends ITreeIndex> treeFactory, ArrayList<ComparableFileName> allFiles)
            throws HyracksDataException {
        String[] files = baseDir.getFile().list(filter);
        for (String fileName : files) {
            FileReference fileRef = baseDir.getChild(fileName);
            allFiles.add(new ComparableFileName(fileRef));
        }
    }
}
