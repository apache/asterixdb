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

package org.apache.hyracks.storage.am.rtree.frames;

import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;

public class RTreeNSMLeafFrameFactory extends RTreeFrameFactory {
    private static final long serialVersionUID = 1360338463029768516L;

    public RTreeNSMLeafFrameFactory(RTreeTypeAwareTupleWriterFactory tupleWriterFactory,
            IPrimitiveValueProviderFactory[] keyValueProviderFactories, RTreePolicyType rtreePolicyType,
            boolean isPointMBR) {
        super(tupleWriterFactory, keyValueProviderFactories, rtreePolicyType, isPointMBR);
    }

    @Override
    public IRTreeLeafFrame createFrame() {
        IPrimitiveValueProvider[] keyValueProviders = new IPrimitiveValueProvider[keyValueProviderFactories.length];
        for (int i = 0; i < keyValueProviders.length; i++) {
            keyValueProviders[i] = keyValueProviderFactories[i].createPrimitiveValueProvider();
        }
        return new RTreeNSMLeafFrame(tupleWriterFactory.createTupleWriter(), keyValueProviders, rtreePolicyType,
                isPointMBR);
    }

}
