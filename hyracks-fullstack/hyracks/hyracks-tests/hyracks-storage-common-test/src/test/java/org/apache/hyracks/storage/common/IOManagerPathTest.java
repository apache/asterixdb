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
package org.apache.hyracks.storage.common;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IOManagerPathTest {
    @Test
    public void testPrefixNames() throws HyracksDataException {
        IODeviceHandle shorter = new IODeviceHandle(new File("/tmp/tst/1"), "storage");
        IODeviceHandle longer = new IODeviceHandle(new File("/tmp/tst/11"), "storage");
        IOManager ioManager = new IOManager(Arrays.asList(new IODeviceHandle[] { shorter, longer }),
                new DefaultDeviceResolver(), 2, 10);
        FileReference f = ioManager.resolveAbsolutePath("/tmp/tst/11/storage/Foo_idx_foo/my_btree");
        Assert.assertEquals("/tmp/tst/11/storage/Foo_idx_foo/my_btree", f.getAbsolutePath());
    }

    @Test(expected = HyracksDataException.class)
    public void testDuplicates() throws HyracksDataException {
        IODeviceHandle first = new IODeviceHandle(new File("/tmp/tst/1"), "storage");
        IODeviceHandle second = new IODeviceHandle(new File("/tmp/tst/1"), "storage");
        IOManager ioManager = new IOManager(Arrays.asList(new IODeviceHandle[] { first, second }),
                new DefaultDeviceResolver(), 2, 19);
    }

    @After
    @Before
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/tst/"));
    }

}
