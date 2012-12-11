/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMIndexFileManager extends AbstractLSMIndexFileManager {

    public LSMIndexFileManager(IIOManager ioManager, IFileMapProvider fileMapProvider, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> treeFactory, int startIODeviceIndex) {
        super(ioManager, fileMapProvider, file, treeFactory, startIODeviceIndex);
    }

    protected void cleanupAndGetValidFilesInternal(IODeviceHandle dev, FilenameFilter filter,
            TreeIndexFactory<? extends ITreeIndex> treeFactory, ArrayList<ComparableFileName> allFiles)
            throws HyracksDataException, IndexException {
        File dir = new File(dev.getPath(), baseDir);
        String[] files = dir.list(filter);
        for (String fileName : files) {
            File file = new File(dir.getPath() + File.separator + fileName);
            FileReference fileRef = new FileReference(file);
            if (isValidTreeIndex(treeFactory.createIndexInstance(fileRef))) {
                allFiles.add(new ComparableFileName(fileRef));
            } else {
                file.delete();
            }
        }
    }
}
