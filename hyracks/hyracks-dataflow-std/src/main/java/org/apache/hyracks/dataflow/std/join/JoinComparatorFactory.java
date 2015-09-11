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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;

public class JoinComparatorFactory implements ITuplePairComparatorFactory {
    private static final long serialVersionUID = 1L;

    private final IBinaryComparatorFactory bFactory;
    private final int pos0;
    private final int pos1;

    public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
        this.bFactory = bFactory;
        this.pos0 = pos0;
        this.pos1 = pos1;
    }

    @Override
    public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
        return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
    }

}
