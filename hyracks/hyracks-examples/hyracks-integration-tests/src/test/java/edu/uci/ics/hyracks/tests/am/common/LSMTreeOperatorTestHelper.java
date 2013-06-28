/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.tests.am.common;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Date;

import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.test.support.TestVirtualBufferCacheProvider;

public class LSMTreeOperatorTestHelper extends TreeOperatorTestHelper {
    protected static int DEFAULT_MEM_PAGE_SIZE = 32768;
    protected static int DEFAULT_MEM_NUM_PAGES = 1000;
    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    protected final IOManager ioManager;
    protected final IVirtualBufferCacheProvider virtualBufferCacheProvider;

    public LSMTreeOperatorTestHelper(IOManager ioManager) {
        this.ioManager = ioManager;
        this.virtualBufferCacheProvider = new TestVirtualBufferCacheProvider(DEFAULT_MEM_PAGE_SIZE,
                DEFAULT_MEM_NUM_PAGES);
    }

    public String getPrimaryIndexName() {
        return "primary" + simpleDateFormat.format(new Date());
    }

    public String getSecondaryIndexName() {
        return "secondary" + simpleDateFormat.format(new Date());
    }

    @Override
    public void cleanup(String primaryFileName, String secondaryFileName) {
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            File primaryDir = new File(dev.getPath(), primaryFileName);
            cleanupDir(primaryDir);
            File secondaryDir = new File(dev.getPath(), secondaryFileName);
            cleanupDir(secondaryDir);
        }
    }

    private void cleanupDir(File dir) {
        if (!dir.exists()) {
            return;
        }
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        if (files != null) {
            for (String fileName : files) {
                File file = new File(dir.getPath() + File.separator + fileName);
                file.delete();
            }
        }
        dir.delete();
    }
}
