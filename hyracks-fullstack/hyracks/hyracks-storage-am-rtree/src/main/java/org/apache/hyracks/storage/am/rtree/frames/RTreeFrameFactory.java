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

import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;

public abstract class RTreeFrameFactory implements ITreeIndexFrameFactory {
    private static final long serialVersionUID = 1L;
    protected final RTreeTypeAwareTupleWriterFactory tupleWriterFactory;
    protected final IPrimitiveValueProviderFactory[] keyValueProviderFactories;
    protected final RTreePolicyType rtreePolicyType;
    protected final boolean isPointMBR;

    public RTreeFrameFactory(RTreeTypeAwareTupleWriterFactory tupleWriterFactory,
            IPrimitiveValueProviderFactory[] keyValueProviderFactories, RTreePolicyType rtreePolicyType,
            boolean isPointMBR) {
        this.tupleWriterFactory = tupleWriterFactory;
        if (keyValueProviderFactories.length % 2 != 0) {
            throw new IllegalArgumentException("The key has different number of dimensions.");
        }
        this.keyValueProviderFactories = keyValueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.isPointMBR = isPointMBR;
    }

    @Override
    public RTreeTypeAwareTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }

}
