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

package org.apache.hyracks.tests.am.common;

import java.util.Collections;
import java.util.Map;

import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.test.support.TestVirtualBufferCacheProvider;

public class LSMTreeOperatorTestHelper extends TreeOperatorTestHelper {
    public static final int DEFAULT_MEM_PAGE_SIZE = 32768;
    public static final int DEFAULT_MEM_NUM_PAGES = 1000;
    public static final double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    public static final ConstantMergePolicyFactory MERGE_POLICY_FACTORY = new ConstantMergePolicyFactory();
    public static final Map<String, String> MERGE_POLICY_PROPERTIES =
            Collections.singletonMap(ConstantMergePolicyFactory.NUM_COMPONENTS, "3");
    public static final boolean DURABLE = false;
    protected final IOManager ioManager;
    protected final IVirtualBufferCacheProvider virtualBufferCacheProvider;

    public LSMTreeOperatorTestHelper(IOManager ioManager) {
        this.ioManager = ioManager;
        this.virtualBufferCacheProvider =
                new TestVirtualBufferCacheProvider(DEFAULT_MEM_PAGE_SIZE, DEFAULT_MEM_NUM_PAGES);
    }

    public IVirtualBufferCacheProvider getVirtualBufferCacheProvider() {
        return virtualBufferCacheProvider;
    }
}
