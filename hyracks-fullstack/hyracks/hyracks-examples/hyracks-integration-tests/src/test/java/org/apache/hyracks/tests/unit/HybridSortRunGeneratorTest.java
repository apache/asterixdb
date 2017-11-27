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

package org.apache.hyracks.tests.unit;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.HybridTopKSortRunGenerator;

public class HybridSortRunGeneratorTest extends AbstractRunGeneratorTest {
    @Override
    AbstractSortRunGenerator[] getSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int numOfInputRecord)
            throws HyracksDataException {
        HybridTopKSortRunGenerator runGenerator = new HybridTopKSortRunGenerator(ctx, frameLimit, numOfInputRecord,
                SortFields, null, ComparatorFactories, RecordDesc);
        HybridTopKSortRunGenerator runGeneratorWithOneNormalizedKey =
                new HybridTopKSortRunGenerator(ctx, frameLimit, numOfInputRecord, SortFields,
                        new INormalizedKeyComputerFactory[] { new IntegerNormalizedKeyComputerFactory() },
                        ComparatorFactories, RecordDesc);
        HybridTopKSortRunGenerator runGeneratorWithNormalizedKeys = new HybridTopKSortRunGenerator(ctx, frameLimit,
                numOfInputRecord, SortFields, new INormalizedKeyComputerFactory[] {
                        new IntegerNormalizedKeyComputerFactory(), new UTF8StringNormalizedKeyComputerFactory() },
                ComparatorFactories, RecordDesc);

        return new AbstractSortRunGenerator[] { runGenerator, runGeneratorWithOneNormalizedKey,
                runGeneratorWithNormalizedKeys };
    }
}
